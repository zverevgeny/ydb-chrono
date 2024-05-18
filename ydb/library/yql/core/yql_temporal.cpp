#include "yql_temporal.h"
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {

namespace {

using namespace NNodes;

//TODO use period details from the table meta info
//currently use a convention to derive all names from a period's name
struct TPeriodInfo {
    TPeriodInfo(TStringBuf name)
        : Name(name)
        , StartColumn(Name + "_start")
        , EndColumn(Name + "_end")
        , TreeNodeColumn(Name + "_tree_node")
        , StartIndex(StartColumn + "_idx")
        , EndIndex(EndColumn + "_idx")
    {}
        
    const TString Name;
    const TString StartColumn;
    const TString EndColumn;
    const TString TreeNodeColumn;
    const TString StartIndex;
    const TString EndIndex;
};

//TODO probable there is such a function in a common place
TExprNode::TPtr GetValueByName(TExprNode::TPtr list, TStringBuf name) {
    //YQL_ENSURE(list->IsList());
    for (auto nv: list->Children()) {
        if (nv->IsList() && nv->ChildrenSize() == 2) {
            auto n = nv->ChildPtr(0);
            auto v = nv->ChildPtr(1);
            YQL_ENSURE(n->IsAtom());
            if (n->Content() == name) {
                return v;
            }
        }
    }
    return  {};
}

//repalce with newValue or remove when newValue is null
TExprNode::TPtr ReplaceValueByName(TExprNode::TPtr list, TExprContext& ctx, TStringBuf oldName, TStringBuf newName, TExprNode::TPtr newValue) {
    auto pos = list->Pos();
    YQL_ENSURE(list->IsList());
    TVector<TExprNode::TPtr> newItems;
    newItems.reserve(list->ChildrenSize());
    for (auto nv: list->Children()) {
        YQL_ENSURE(nv->IsList() && nv->ChildrenSize() == 2);
        auto n = nv->ChildPtr(0);
        auto v = nv->ChildPtr(1);
        YQL_ENSURE(n->IsAtom());
        if (n->Content() == oldName) {
            if (newValue) {
                newItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(newName)
                    .Value(newValue)
                    .Done().Ptr()
                );
            }
        } else {
            newItems.push_back(nv);
        }
    }
    return ctx.NewList(pos, std::move(newItems));
}

TExprNode::TPtr AppendItem(const TExprNode::TPtr list, TExprContext& ctx, TStringBuf name, TExprNode::TPtr newValue) {
    auto pos = list->Pos();
    auto newItems = list->ChildrenList();
    newItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(name)
        .Value(newValue)
        .Done().Ptr()
    );
    return ctx.NewList(pos, std::move(newItems));
}


std::optional<std::pair<TPeriodInfo, TExprNode::TPtr>> IsFilterOverIndexedPeriod(const TCoFilterBase& node) {
    auto input = node.Input();
    if (auto right = input.Maybe<TCoRight>()) {
        input = right.Cast().Input();
    } else {
        return std::nullopt;
    }
    if (input.Raw()->ChildrenSize() < 5) {
        return std::nullopt;
    }
    auto periodTree = GetValueByName(input.Raw()->ChildPtr(4), "period_tree");
    if (!periodTree) {
        return std::nullopt;
    }
    auto periodTreeColumn = periodTree->Content();
    auto periodName = periodTreeColumn.SubString(0, periodTreeColumn.find('_'));
    std::optional<TPeriodInfo> period = TPeriodInfo{periodName};
    
    auto lambdaArg = node.Lambda().Args().Raw()->Child(0);
    auto lambdaBody = node.Lambda().Body();
    if (!lambdaBody.Maybe<TCoAnd>() || lambdaBody.Raw()->ChildrenSize() != 2) {
        return std::nullopt;
    }
    TExprNode::TPtr lower;
    TExprNode::TPtr upper;
    for(size_t i = 0; i != 2; ++i) {
        auto condition = lambdaBody.Raw()->Child(i);
        if (condition->IsCallable("Coalesce")) {
            condition = condition->Child(0);
        }
        if (condition->IsCallable("<=")) {
            auto lhs = condition->Child(0);
            auto rhs = condition->Child(1);
            if (lhs->IsCallable("Member") && lhs->Child(0) == lambdaArg && lhs->Child(1)->Content() == period->StartColumn) {
                lower = rhs;
            }
        } else if (condition->IsCallable("<")) {
            auto lhs = condition->Child(0);
            auto rhs = condition->Child(1);
            if (rhs->IsCallable("Member") && rhs->Child(0) == lambdaArg && rhs->Child(1)->Content() == period->EndColumn) {
               upper = lhs;
            }
        }
    }
    if (lower && lower == upper) {
        return {std::pair{*period, lower}};
    }
    return std::nullopt;
}

std::vector<ui32> Pwr2Desc0(int height) {
    std::vector<ui32> result;
    height++;
    while (height-->0) {
        result.push_back((ui64(1) << height) / 2); //...4, 2, 1, 0
    }
    return result;
}

template<typename T>
TCoAsList MakeLiteralList(const std::vector<ui32>& ds, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> exprs;
    
    for(auto d: ds){
        TExprBase e = Build<T>(ctx, pos).Literal().Build(ToString(d)).Done();
        exprs.push_back(e);
    }
    auto list = Build<TCoAsList>(ctx, pos)
        .Add(exprs)
        .Done();
    return list;
}


TCoLambda MakeForkNodeLambda(TExprContext& ctx, const TPositionHandle& pos) {
    auto treeHeight = 31;
    // C++
        //     auto node = ui32(1<<31);
        // for (auto step = node / 2; step >= 1; step /= 2) {
        // 	if (node > end) {
        // 		node -= step;
        // 	} else if (node < start) {
        // 		node += step;
        // 	} else {
        // 		break;
        // 	}
        // }
        // return node;
    auto pwr2Desc0 = MakeLiteralList<TCoUint32>(Pwr2Desc0(treeHeight), ctx, pos);
    auto startArg = ctx.NewArgument(pos, "start");
    auto endArg = ctx.NewArgument(pos, "end");
    //Convert args to Uint32, do all math in ints, then convert back to Datetime
    auto start = Build<TCoBitCast>(ctx, startArg->Pos())
        .Value(startArg)
        .Type().Build("Uint32")
        .Done()
    .Ptr();
    auto end = Build<TCoBitCast>(ctx, endArg->Pos())
        .Value(endArg)
        .Type().Build("Uint32")
        .Done()
    .Ptr();
    auto fold = Build<TCoFold>(ctx, pos)
        .Input(pwr2Desc0)
        .State<TCoUint32>()
            .Literal().Build(ToString(1u << treeHeight))
            .Build()
        .UpdateHandler<TCoLambda>()
            .Args({"shift", "root"})
            .Body<TCoIf>()
                .Predicate<TCoCmpLess>()
                    .Left("root")
                    .Right(start)
                    .Build()
                .ThenValue<TCoAdd>()
                        .Left("root")
                        .Right("shift")
                    .Build()
                .ElseValue<TCoIf>()
                    .Predicate<TCoCmpLessOrEqual>()
                        .Left(end)
                        .Right("root")
                        .Build()
                    .ThenValue<TCoSub>()
                        .Left("root")
                        .Right("shift")
                        .Build()
                    .ElseValue("root")
                    .Build()
                .Build()
            .Build()
        .Done();

    auto expr = Build<TCoIf>(ctx, end->Pos())
        .Predicate<TCoCmpEqual>()
            .Left(end)
            .Right<TCoUint32>()
                .Literal().Build("1")
                .Build()
            .Build()
        .ThenValue<TCoDatetime>()
            .Literal().Build("0")
            .Build()
        .ElseValue<TCoUnwrap>()
           .Optional<TCoSafeCast>()
            .Value(fold.Ptr())
            .Type<TCoAtom>().Build("Datetime")
                .Build()
            .Build()
        .Done();
    return Build<TCoLambda>(ctx, pos)
        .Args({startArg, endArg})
        .Body(expr)
        .Done();
}

//Filter out ints that are not valid datetime and convert the rest to Datetime
TExprNode::TPtr MakeSafeConvertUint32ListToDatetimeList(TExprContext& ctx, TExprNode::TPtr list) {
    const ui32 maxDatetime = 4102444800; //2100-01-01T00:00:00Z
    return Build<TCoMap>(ctx, list->Pos())
        .Input<TCoFilter>()
            .Input(list)
            .Lambda<TCoLambda>()
                .Args({"x"})
                .Body<TCoCmpLess>()
                    .Left("x")
                    .Right<TCoUint32>()
                        .Literal().Build(ToString(maxDatetime))
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Lambda<TCoLambda>()
            .Args({"x"})
            .Body<TCoUnwrap>()
                .Optional<TCoSafeCast>()
                    .Value("x")
                    .Type<TCoAtom>().Build("Datetime")
                    .Build()
                .Build()
            .Build()
        .Done()
    .Ptr();
}


enum class ELeftOrRight {
    Left,
    Right
};

TExprNode::TPtr MakeLeftRightNodesExpr(TExprContext& ctx, TExprNode::TPtr point, ELeftOrRight leftOrRight) {
    // C++
        // TVector<ui32> result;
        // auto node = ui32(1 << 31);
        // for (auto step = node / 2; step >= 1; step /= 2) {
        //     if (node > point) {
        //         node -= step;
        //     } else if (node < point) {
        //         result.push_back(node);
        //         node += step;
        //     } else {
        //         break;
        //     }
        // }
	    // return result;
    const auto pos = point->Pos();
    auto treeHeight = 31;
    auto pwr2Desc0 = MakeLiteralList<TCoUint32>(Pwr2Desc0(treeHeight), ctx, pos);

    auto second = Build<TCoLambda>(ctx, pos)
        .Args({"ab"})
        .Body<TCoNth>()
            .Tuple("ab")
            .Index().Build("1")
        .Build()
    .Done();

    auto append = Build<TCoLambda>(ctx, pos)
        .Args({"ab"})
        .Body<TCoExtend>()
            .Add<TCoNth>()
                .Tuple("ab")
                .Index().Build("1")
                .Build()
            .Add<TCoAsList>()
                .Add<TCoNth>()
                    .Tuple("ab")
                    .Index().Build("0")
                .Build()
            .Build()
        .Build()
    .Done();

    auto [left, right] = leftOrRight == ELeftOrRight::Left ? std::pair{append, second} : std::pair{second, append}; 
    //Convert input point to Uint32, make all math in integers, then convert the result back to Datetime            
    point = Build<TCoBitCast>(ctx, pos)
        .Value(point)
        .Type().Build("Uint32")
        .Done()
    .Ptr();
    auto binarySearch = Build<TCoFold>(ctx, pos)
        .Input(pwr2Desc0)
        .State<TExprList>()
            .Add<TCoUint32>()
                .Literal().Build(ToString(1u << treeHeight))
                .Build()
            .Add<TCoList>()
                .ListType<TCoListType>()
                    .ItemType<TCoDataType>()
                        .Type().Build("Uint32")
                        .Build()
                    .Build()
                .Build()
            .Build()
        .UpdateHandler<TCoLambda>()
            .Args({"shift", "state"})
            .Body<TCoIf>()
                .Predicate<TCoCmpLess>()
                    .Left(point)
                    .Right<TCoNth>()
                        .Tuple("state")
                        .Index().Build("0")
                        .Build()
                    .Build()
                .ThenValue<TExprList>()
                    .Add<TCoSub>()
                        .Left<TCoNth>()
                            .Tuple("state")
                            .Index().Build("0")
                            .Build()
                        .Right("shift")
                        .Build()
                    .Add<TExprApplier>()
                        .Apply(right)
                        .With(0, "state")
                        .Build()
                    .Build()
                .ElseValue<TCoIf>()
                    .Predicate<TCoCmpLess>()
                        .Left<TCoNth>()
                            .Tuple("state")
                            .Index().Build("0")
                            .Build()
                        .Right(point)
                        .Build()
                    .ThenValue<TExprList>()
                        .Add<TCoAdd>()
                            .Left<TCoNth>()
                                .Tuple("state")
                                .Index().Build("0")
                                .Build()
                            .Right("shift")
                            .Build()
                        .Add<TExprApplier>()
                            .Apply(left)
                            .With(0, "state")
                            .Build()
                        .Build()
                    .ElseValue("state")
                    .Build()
                .Build()
            .Build()
        .Done();

    return MakeSafeConvertUint32ListToDatetimeList(ctx, Build<TCoNth>(ctx, pos)
            .Tuple(binarySearch)
            .Index().Build("1")
        .Done().Ptr()
    );
}


} //namespace


std::optional<TString> GetPeriodIndexNameFromReadSettings(const TExprNode::TPtr& settings) {
    if (auto indexName = GetValueByName(settings, "period_index")) {
        YQL_ENSURE(indexName->IsAtom());
        return TString{indexName->Content()};
    }
    return std::nullopt;
}

TExprNode::TPtr RemovePeriodIndexNameFromReadSettings(const TExprNode::TPtr& settings, TExprContext& ctx) {
    return ReplaceValueByName(settings, ctx, "period_index", "period_index", {});
}

IGraphTransformer::TStatus RewriteIOForTemporal(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TTypeAnnotationContext& types, TExprContext& ctx) {
    Y_UNUSED(types);
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = false;
    bool restart = false;
    auto ret = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        YQL_ENSURE(node->Type() == TExprNode::Callable);
        const auto name = node->Content();
        if (name == LeftName || name == RightName) {
            auto child = node->ChildPtr(0);
            if (child->IsCallable(ReadName)) {
                auto read = child;
                auto last = read->ChildrenSize() - 1;
                auto options = read->ChildPtr(last);
                if (auto period = GetValueByName(options, "period")) {
                    YQL_ENSURE(period->IsAtom());
                    auto periodName = period->Content();
                    YQL_CLOG(DEBUG, Core) << "Rewrite " << node->Content() << " for period: " << periodName;
                    auto periodInfo = TPeriodInfo{periodName};
                    auto pos = read->Pos();                    
                    options = AppendItem(options, ctx, "period_tree", ctx.NewAtom(pos, periodInfo.TreeNodeColumn));
                    auto read2 = ctx.ShallowCopy(*read);
                    read = ctx.ChangeChild(*read, last, ReplaceValueByName(options, ctx, "period", "period_index", ctx.NewAtom(pos, periodInfo.StartIndex)));
                    read2 = ctx.ChangeChild(*read2, last, ReplaceValueByName(options, ctx, "period", "period_index", ctx.NewAtom(pos, periodInfo.EndIndex)));
                    read2 = ctx.ChangeChild(
                        *read2, 
                        0,
                        Build<TCoLeft>(ctx, read->Pos())
                            .Input(read)
                        .Done().Ptr()
                    );
                    restart = true;
                    return ctx.ChangeChild(*node, 0, std::move(read2));
                }
            }
        } else if (name == WriteName) {
            auto settings = node->Child(4);
            if (auto period = GetValueByName(settings, "period")) {
                TPeriodInfo periodInfo(period->Content());
                YQL_CLOG(INFO, Core) << "Rewrite " << node->Content() << " over indexed period: " << periodInfo.Name;
                auto values = node->ChildPtr(3);
                auto pos = values->Pos();

                if (values->Child(0)->IsList()) {
                    auto items = values->Child(0)->ChildrenList();
                    values = ctx.Builder(pos)
                        .Callable("PersistableRepr")
                        .Add(0, ctx.Builder(pos)
                                .Callable("AsList")
                                .Add(std::move(items))
                                .Seal()
                                .Build()
                        )
                        .Seal()
                        .Build();
                }
                if (values->IsCallable("AssumeColumnOrder")) {
                    auto columns = values->Child(1)->ChildrenList();
                    columns.push_back(ctx.NewAtom(values->Pos(), periodInfo.TreeNodeColumn));
                    values =  ctx.ChangeChild(
                        *values,
                        1,
                        ctx.ChangeChildren(*values->Child(1), std::move(columns))
                    );
                }
                auto settings = node->Child(4);
                return ctx.ChangeChild(
                    *ctx.ChangeChild(
                        *node, 
                        3, 
                        ctx.ChangeChild(*values, 0, Build<TCoMap>(ctx, pos)
                            .Input(values->Child(0))
                            .Lambda<TCoLambda>()
                                .Args({"item"})
                                .Body<TCoAddMember>()
                                    .Struct("item")
                                    .Name().Build(periodInfo.TreeNodeColumn)
                                    .Item<TExprApplier>()
                                        .Apply(MakeForkNodeLambda(ctx, pos))
                                        .With<TCoMember>(0)
                                            .Struct("item")
                                            .Name().Build(periodInfo.StartColumn)
                                            .Build()
                                        .With<TCoMember>(1)
                                            .Struct("item")
                                            .Name().Build(periodInfo.EndColumn)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Done().Ptr()
                        )
                    ),
                    4, 
                    ReplaceValueByName(settings, ctx, "period", "", {})
                );
            }
        }
        return node;
    }, ctx, settings);
    if (restart) {
        ret.HasRestart = true;
    }
    return ret;
}


TExprNode::TPtr TryRewriteFilterOverIndexedPeriod(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    auto period = IsFilterOverIndexedPeriod(TCoFilterBase(node));
    if (!period) {
        return node;
    }
    const auto& periodInfo = period->first;
    const auto& point = period->second;
    YQL_CLOG(INFO, Core) << "Rewrite " << node->Content() << " over indexed period: " << periodInfo.Name;
    const auto pos = node->Pos();
    auto toIndexedRead = node->Child(0)->Child(0);
    auto fromIndexedRead = toIndexedRead->Child(0)->Child(0);
    TVector<TCoNameValueTuple> sqlInOptions;
    auto leftLambda = Build<TCoLambda>(ctx, pos)
        .Args({"row"})
        .Body<TCoCoalesce>()
            .Predicate<TCoAnd>()
                .Add<TCoSqlIn>()
                    .Collection((MakeLeftRightNodesExpr(ctx, point, ELeftOrRight::Left)))
                    .Lookup<TCoMember>()
                        .Struct("row")
                        .Name().Build(periodInfo.TreeNodeColumn)
                        .Build()
                    .Options()
                        .Add(sqlInOptions)
                        .Build()
                    .Build()
                .Add<TCoCmpLess>()
                    .Left(point)
                    .Right<TCoMember>()
                        .Struct("row")
                        .Name().Build(periodInfo.EndColumn)
                        .Build()
                    .Build()
                .Build()
            .Value<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .Done();
    
    auto rightLambda = Build<TCoLambda>(ctx, pos)
        .Args({"row"})
        .Body<TCoCoalesce>()
            .Predicate<TCoAnd>()
                .Add<TCoSqlIn>()
                    .Collection((MakeLeftRightNodesExpr(ctx, point, ELeftOrRight::Right)))
                    .Lookup<TCoMember>()
                        .Struct("row")
                        .Name().Build(periodInfo.TreeNodeColumn)
                        .Build()
                    .Options()
                        .Add(sqlInOptions)
                        .Build()
                    .Build()
                .Add<TCoCmpLessOrEqual>()
                    .Left<TCoMember>()
                        .Struct("row")
                        .Name().Build(periodInfo.StartColumn)
                        .Build()
                    .Right(point)
                    .Build()
                .Build()
            .Value<TCoBool>()
                .Literal().Build("false")
                .Build()
             .Build()
        .Done();

    auto centerLambda = Build<TCoLambda>(ctx, pos)
        .Args({"row"})
        .Body<TCoCoalesce>()
            .Predicate<TCoCmpEqual>()
                .Left<TCoMember>()
                    .Struct("row")
                    .Name().Build(periodInfo.TreeNodeColumn)
                    .Build()
                .Right(point)
                .Build()
            .Value<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .Done();

    auto unionAll = Build<TCoUnionAll>(ctx, pos)
        .Add<TCoFilter>()
            .Input<TCoRight>()
                .Input(toIndexedRead)
                .Build()
            .Lambda(leftLambda)
            .Build()    
        .Add<TCoFilter>()
            .Input<TCoRight>()
                .Input(fromIndexedRead)
                .Build()
            .Lambda(rightLambda)
            .Build()
        .Add<TCoFilter>()
            .Input<TCoRight>()
                .Input(fromIndexedRead)
                .Build()
            .Lambda(centerLambda)
            .Build()
    .Done();
    return unionAll.Ptr();
}

} // namespace NYql
