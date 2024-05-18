#pragma once

#include <ydb/library/yql/core/common_opt/yql_co.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

std::optional<TString> GetPeriodIndexNameFromReadSettings(const TExprNode::TPtr& settings);

TExprNode::TPtr RemovePeriodIndexNameFromReadSettings(const TExprNode::TPtr& settings, TExprContext& ctx);

IGraphTransformer::TStatus RewriteIOForTemporal(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TTypeAnnotationContext& types, TExprContext& ctx);

TExprNode::TPtr TryRewriteFilterOverIndexedPeriod(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);


} // namespace NYql
