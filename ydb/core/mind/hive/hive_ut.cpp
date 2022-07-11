#include <math.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/mediator/mediator.h>

#include <library/cpp/actors/interconnect/interconnect_impl.h>

#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <util/random/entropy.h>
#include <util/stream/null.h>
#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/system/sanitizers.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

const bool STRAND_PDISK = true;
#ifndef NDEBUG
static constexpr bool ENABLE_DETAILED_HIVE_LOG = true;
#else
static constexpr bool ENABLE_DETAILED_HIVE_LOG = false;
#endif
const char *DOMAIN_NAME = "dc-1";

namespace NKikimr {
namespace {
    using namespace NActors;

    void SetupLogging(TTestActorRuntime& runtime) {
        NActors::NLog::EPriority priority = ENABLE_DETAILED_HIVE_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
        NActors::NLog::EPriority otherPriority = NLog::PRI_ERROR;

        if (ENABLE_DETAILED_HIVE_LOG) {
            runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_TRACE);
        } else {
            runtime.SetLogPriority(NKikimrServices::HIVE, priority);
            runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, priority);
        }
        runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::LOCAL, priority);
        runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, otherPriority);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_NODE, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_SYNCLOG, NLog::PRI_CRIT);
        runtime.SetLogPriority(NKikimrServices::BS_SYNCER, NLog::PRI_CRIT);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_GET, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_COLLECT, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_BLOCK, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_RANGE, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_DISCOVER, otherPriority);
        runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, otherPriority);
        runtime.SetLogPriority(NKikimrServices::PIPE_SERVER, otherPriority);
        runtime.SetLogPriority(NKikimrServices::TX_DUMMY, otherPriority);
        runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, otherPriority);
        runtime.SetLogPriority(NKikimrServices::STATESTORAGE, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, otherPriority);
    }

    THashMap<ui32, TIntrusivePtr<TNodeWardenConfig>> NodeWardenConfigs;

    void SetupDomainInfo(TTestActorRuntime &runtime, TAppPrepare &app)  {
        app.ClearDomainsAndHive();

        ui32 domainUid = TTestTxConfig::DomainUid;
        ui32 ssId = 0;
        ui32 planResolution = 50;
        ui64 schemeRoot = TTestTxConfig::SchemeShard;
        ui64 hive = MakeDefaultHiveID(ssId);
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                    DOMAIN_NAME, domainUid, schemeRoot,
                    ssId, ssId, TVector<ui32>{ssId},
                    domainUid, TVector<ui32>{domainUid},
                    planResolution,
                    TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(domainUid, 1)},
                    TVector<ui64>{},
                    TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(domainUid, 1)},
                    DefaultPoolKinds(2));

        TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
        ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
        runtime.SetTxAllocatorTabletIds(ids);

        app.AddDomain(domain.Release());
        app.AddHive(domainUid, hive);
    }

    static TString STORAGE_POOL = "def";

    void SetupChannels(TAppPrepare &app) {
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
        for (ui32 channelIdx = 0; channelIdx < 3; ++channelIdx) {
            profile.Channels.push_back(
                TChannelProfiles::TProfile::TChannel(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default));
        }
        app.SetChannels(std::move(channelProfiles));
    }

    static TChannelBind GetChannelBind(const TString& storagePool) {
        TChannelBind bind;
        bind.SetStoragePoolName(storagePool);
        return bind;
    }

    static TChannelsBindings BINDED_CHANNELS = {GetChannelBind(STORAGE_POOL + "1"), GetChannelBind(STORAGE_POOL + "2"), GetChannelBind(STORAGE_POOL + "3")};

    void SetupNodeWarden(TTestActorRuntime &runtime) {
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            TString staticConfig(
                "AvailabilityDomains: 0 "
                "PDisks { NodeID: $Node1 PDiskID: 1 PDiskGuid: 1 Path: \"/tmp/pdisk.dat\" }"
                "VDisks { VDiskID { GroupID: 0 GroupGeneration: 1 Ring: 0 Domain: 0 VDisk: 0 }"
                "    VDiskLocation { NodeID: $Node1 PDiskID: 1 PDiskGuid: 1 VDiskSlotID: 0 }"
                "}"
                "Groups { GroupID: 0 GroupGeneration: 1 ErasureSpecies: 0 "// None
                "    Rings {"
                "        FailDomains { VDiskLocations { NodeID: $Node1 PDiskID: 1 VDiskSlotID: 0 PDiskGuid: 1 } }"
                "    }"
                "}");

            SubstGlobal(staticConfig, "$Node1", Sprintf("%" PRIu32, runtime.GetNodeId(0)));

            TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig = new TNodeWardenConfig(
                    STRAND_PDISK && !runtime.IsRealThreads() ? static_cast<IPDiskServiceFactory*>(new TStrandedPDiskServiceFactory(runtime)) :
                    static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory()));
                //nodeWardenConfig->Monitoring = monitoring;
            google::protobuf::TextFormat::ParseFromString(staticConfig, &nodeWardenConfig->ServiceSet);

            TIntrusivePtr<TNodeWardenConfig> existingNodeWardenConfig = NodeWardenConfigs[nodeIndex];
            if (existingNodeWardenConfig != nullptr) {
                std::swap(nodeWardenConfig->SectorMaps, existingNodeWardenConfig->SectorMaps);
            }

            NodeWardenConfigs[nodeIndex] = nodeWardenConfig;
        }
    }

    void SetupPDisk(TTestActorRuntime &runtime) {
        if (runtime.GetNodeCount() == 0)
            return;

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig = NodeWardenConfigs[0];

        TString pDiskPath;
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap;
        ui64 pDiskSize = 32ull << 30ull;
        ui64 pDiskChunkSize = 32u << 20u;
        if (true /*in memory*/) {
            pDiskPath = "/tmp/pdisk.dat";
            auto& existing = nodeWardenConfig->SectorMaps[pDiskPath];
            if (existing && existing->DeviceSize == pDiskSize) {
                sectorMap = existing;
            } else {
                sectorMap.Reset(new NPDisk::TSectorMap(pDiskSize));
                nodeWardenConfig->SectorMaps[pDiskPath] = sectorMap;
            }
        } else {
            static TTempDir tempDir;
            pDiskPath = tempDir() + "/pdisk.dat";
        }
        nodeWardenConfig->ServiceSet.MutablePDisks(0)->SetPath(pDiskPath);
        ui64 pDiskGuid = 1;
        static ui64 iteration = 0;
        ++iteration;
        FormatPDisk(
                    pDiskPath,
                    pDiskSize,
                    4 << 10,
                    pDiskChunkSize,
                    pDiskGuid,
                    0x1234567890 + iteration,
                    0x4567890123 + iteration,
                    0x7890123456 + iteration,
                    NPDisk::YdbDefaultPDiskSequence,
                    TString(""),
                    false,
                    false,
                    sectorMap);
    }

    void SetupLocals(TTestActorRuntime &runtime, bool isLocalEnabled) {
        if (!isLocalEnabled) {
            return;
        }

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            TLocalConfig::TPtr localConfig(new TLocalConfig());
            localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(
                        &CreateFlatDummyTablet,
                        TMailboxType::Simple, 0,
                        TMailboxType::Simple, 0);
            localConfig->TabletClassInfo[TTabletTypes::Hive].SetupInfo = new TTabletSetupInfo(
                        &CreateDefaultHive,
                        TMailboxType::Simple, 0,
                        TMailboxType::Simple, 0);
            localConfig->TabletClassInfo[TTabletTypes::Mediator].SetupInfo = new TTabletSetupInfo(
                        &CreateTxMediator,
                        TMailboxType::Simple, 0,
                        TMailboxType::Simple, 0);
            TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
            tenantPoolConfig->AddStaticSlot(DOMAIN_NAME);

            runtime.AddLocalService(MakeTenantPoolRootID(), TActorSetupCmd(
                CreateTenantPool(tenantPoolConfig), TMailboxType::Revolving, 0), nodeIndex);
        }
    }

    void EnableSchedule(TTestActorRuntime &runtime, bool isLocalEnabled) {
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            if (isLocalEnabled) {
                TActorId localActor = runtime.GetLocalServiceId(MakeLocalID(runtime.GetNodeId(nodeIndex)), nodeIndex);
                runtime.EnableScheduleForActor(localActor, true);
            }
            runtime.EnableScheduleForActor(runtime.GetLocalServiceId(MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)), nodeIndex), true);
            runtime.EnableScheduleForActor(runtime.GetLocalServiceId(MakeTabletResolverID(), nodeIndex), true);
        }
    }

    void SetupServices(TTestActorRuntime &runtime, bool isLocalEnabled) {
        TAppPrepare app;

        SetupDomainInfo(runtime, app);
        SetupChannels(app);

        app.SetMinRequestSequenceSize(10); // for smaller sequences and high interaction between root and domain hives
        app.SetRequestSequenceSize(10);
        app.SetHiveStoragePoolFreshPeriod(0);

        SetupNodeWarden(runtime);
        SetupPDisk(runtime);

        SetupLocals(runtime, isLocalEnabled);

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            SetupStateStorage(runtime, nodeIndex);
            SetupBSNodeWarden(runtime, nodeIndex, NodeWardenConfigs[nodeIndex]);
            SetupTabletResolver(runtime, nodeIndex);
            SetupNodeWhiteboard(runtime, nodeIndex);
        }

        runtime.Initialize(app.Unwrap());

        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            auto it = NodeWardenConfigs.find(nodeIndex);
            if (it != NodeWardenConfigs.end()) {
                runtime.GetAppData(nodeIndex).StaticBlobStorageConfig = MakeHolder<NKikimrBlobStorage::TNodeWardenServiceSet>(it->second->ServiceSet);
            }
        }

        EnableSchedule(runtime, isLocalEnabled);

        const ui32 domainsNum = 1;
        const ui32 disksInDomain = 1;
        if (!runtime.IsRealThreads()) {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(
                TEvBlobStorage::EvLocalRecoveryDone, domainsNum * disksInDomain));
            runtime.DispatchEvents(options);
        }

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(0), TTabletTypes::BSController),
            &CreateFlatBsController);
    }

    void SetupBoxAndStoragePool(TTestActorRuntime &runtime, ui32 numGroups = 1, const TString& storagePoolNamePrefix = STORAGE_POOL) {
        TActorId sender = runtime.AllocateEdgeActor();
        ui32 domainId = 0;
        ui32 nodeIndex = 0;
        TString pDiskPath;
        if (true /*in memory*/) {
            pDiskPath = "/tmp/pdisk.dat";
        } else {
            pDiskPath = runtime.GetTempDir() + "/pdisk.dat";
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, new TEvInterconnect::TEvListNodes));
        TAutoPtr<IEventHandle> handleNodesInfo;
        auto nodesInfo = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handleNodesInfo);

        auto bsConfigureRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();

        NKikimrBlobStorage::TDefineBox boxConfig;
        boxConfig.SetBoxId(1);

        ui32 nodeId = runtime.GetNodeId(nodeIndex);
        Y_VERIFY(nodesInfo->Nodes[0].NodeId == nodeId);
        auto& nodeInfo = nodesInfo->Nodes[0];

        NKikimrBlobStorage::TDefineHostConfig hostConfig;
        hostConfig.SetHostConfigId(nodeId);
        hostConfig.AddDrive()->SetPath(pDiskPath);
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);

        auto &host = *boxConfig.AddHost();
        host.MutableKey()->SetFqdn(nodeInfo.Host);
        host.MutableKey()->SetIcPort(nodeInfo.Port);
        host.SetHostConfigId(hostConfig.GetHostConfigId());
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineBox()->CopyFrom(boxConfig);

        for (int i = 1; i <= 3; ++i) {
            NKikimrBlobStorage::TDefineStoragePool storagePool;
            storagePool.SetBoxId(1);
            storagePool.SetStoragePoolId(i);
            storagePool.SetName(storagePoolNamePrefix + ToString(i));
            storagePool.SetErasureSpecies("none");
            storagePool.SetVDiskKind("Default");
            storagePool.SetKind("DefaultStoragePool");
            storagePool.SetNumGroups(numGroups);
            storagePool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);
            bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);
        }

        runtime.SendToPipe(MakeBSControllerID(domainId), sender, bsConfigureRequest.Release(), 0, pipeConfig);

        TAutoPtr<IEventHandle> handleConfigureResponse;
        auto configureResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handleConfigureResponse);
        if (!configureResponse->Record.GetResponse().GetSuccess()) {
            Ctest << "\n\n configResponse is #" << configureResponse->Record.DebugString() << "\n\n";
        }
        UNIT_ASSERT(configureResponse->Record.GetResponse().GetSuccess());
    }

    void Setup(TTestActorRuntime& runtime, bool isLocalEnabled = true, ui32 numGroups = 1) {
        using namespace NMalloc;
        TMallocInfo mallocInfo = MallocInfo();
        mallocInfo.SetParam("FillMemoryOnAllocation", "false");
        SetupLogging(runtime);
        SetupServices(runtime, isLocalEnabled);
        SetupBoxAndStoragePool(runtime, numGroups);
    }

    class THiveInitialEventsFilter : TNonCopyable {
        bool IsDone;
    public:
        THiveInitialEventsFilter()
            : IsDone(false)
        {}

        TTestActorRuntime::TEventFilter Prepare() {
            IsDone = false;
            return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                return (*this)(runtime, event);
            };
        }

        bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() == TEvHive::EvCreateTablet) {
                IsDone = true;
                return true;
            }

            return !IsDone;
        }
    };

    class THiveEveryEventFilter : TNonCopyable {
    public:
        THiveEveryEventFilter()
        {}

        TTestActorRuntime::TEventFilter Prepare() {
            return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                return (*this)(runtime, event);
            };
        }

        bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            Y_UNUSED(runtime);
            Y_UNUSED(event);
            return false;
            /*return (event->GetTypeRewrite() >= EventSpaceBegin(TKikimrEvents::ES_HIVE)
                && event->GetTypeRewrite() < EventSpaceEnd(TKikimrEvents::ES_HIVE));*/
        }
    };

}

void FormatPDiskForTest(TString path, ui64 diskSize, ui32 chunkSize, ui64 guid,
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

    NKikimr::FormatPDisk(path, diskSize, 4 << 10, chunkSize, guid,
        chunkKey, logKey, sysLogKey, NPDisk::YdbDefaultPDiskSequence, "", false, false, sectorMap);
}

void InitSchemeRoot(TTestBasicRuntime& runtime, const TActorId& sender) {
    auto evTx = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(1, TTestTxConfig::SchemeShard);
    auto transaction = evTx->Record.AddTransaction();
    transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain);
    transaction->SetWorkingDir("/");
    auto op = transaction->MutableSubDomain();
    op->SetName(DOMAIN_NAME);

    for (const auto& [kind, pool] :runtime.GetAppData().DomainsInfo->GetDomain(0).StoragePoolTypes) {
        auto* p = op->AddStoragePools();
        p->SetKind(kind);
        p->SetName(pool.GetName());
    }

    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, evTx.Release(), 0, GetPipeConfigWithRetries());

    {
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSchemeshardId(), TTestTxConfig::SchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
    }

// there is no coordinators, so transaction is doomed to hung
//
//    auto evSubscribe = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(1);
//    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

//    {
//        TAutoPtr<IEventHandle> handle;
//        auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
//        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 1);
//    }
}

Y_UNIT_TEST_SUITE(THiveTest) {
    template <typename KeyType, typename ValueType>
    static double GetStDev(const THashMap<KeyType, ValueType>& values) {
        double sum = double();
        if (values.empty())
            return sum;
        for (const auto& v : values)
            sum += v.second;
        double mean = sum / values.size();
        sum = double();
        for (const auto& v : values) {
            auto diff = (double)v.second - mean;
            sum += diff * diff;
        }
        auto div = sum / values.size();
        auto st_dev = ::sqrt(div);
        return st_dev;
    }

    template <typename KeyType, typename ValueType>
    static ValueType GetMinMaxDiff(const THashMap<KeyType, ValueType>& values) {
        ValueType minVal = std::numeric_limits<ValueType>::max();
        ValueType maxVal = std::numeric_limits<ValueType>::min();

        if (values.empty()) {
            return std::numeric_limits<ValueType>::max();
        }

        for (const auto& v : values) {
            minVal = std::min(minVal, v.second);
            maxVal = std::max(maxVal, v.second);
        }
        return maxVal - minVal;
    }

    void SendToLocal(TTestActorRuntime &runtime, ui32 nodeIndex, IEventBase* event) {
        TActorId local = MakeLocalID(runtime.GetNodeId(nodeIndex));
        runtime.Send(new IEventHandle(local, TActorId(), event), nodeIndex);
    }

    void SendKillLocal(TTestActorRuntime &runtime, ui32 nodeIndex) {
        SendToLocal(runtime, nodeIndex, new TEvents::TEvPoisonPill());
    }

    void WaitForEvServerDisconnected(TTestActorRuntime &runtime) {
        TDispatchOptions disconnectOptions;
        disconnectOptions.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(TEvTabletPipe::EvServerDisconnected));
        runtime.DispatchEvents(disconnectOptions);
    }

    ui64 SendCreateTestTablet(TTestActorRuntime &runtime, ui64 hiveTablet, ui64 testerTablet,
            THolder<TEvHive::TEvCreateTablet> ev, ui32 nodeIndex, bool doWaitForResult,
            NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, ev.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), expectedStatus,
            (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)expectedStatus);
        UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
            createTabletReply->Record.GetOwner() << " != " << testerTablet);
        ui64 tabletId = createTabletReply->Record.GetTabletID();
        while (doWaitForResult) {
            auto tabletCreationResult = runtime.GrabEdgeEventRethrow<TEvHive::TEvTabletCreationResult>(handle);
            if (tabletId == tabletCreationResult->Record.GetTabletID()) {
                UNIT_ASSERT(tabletCreationResult);
                UNIT_ASSERT_EQUAL_C(tabletCreationResult->Record.GetStatus(), NKikimrProto::OK,
                    (ui32)tabletCreationResult->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
                break;
            }
        }
        return tabletId;
    }

    bool SendDeleteTestTablet(TTestActorRuntime &runtime, ui64 hiveTablet,
            THolder<TEvHive::TEvDeleteTablet> ev, ui32 nodeIndex = 0,
            NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK) {
        bool seenEvDeleteTabletResult = false;
        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletBase::EvDeleteTabletResult) {
                seenEvDeleteTabletResult = true;
            }
            return prevObserverFunc(runtime, event);
        });
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, ev.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto deleteTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvDeleteTabletReply>(handle);
        UNIT_ASSERT(deleteTabletReply);
        UNIT_ASSERT_EQUAL_C(deleteTabletReply->Record.GetStatus(), expectedStatus,
            (ui32)deleteTabletReply->Record.GetStatus() << " != " << (ui32)expectedStatus);
        runtime.SetObserverFunc(prevObserverFunc);
        return seenEvDeleteTabletResult;
    }

    bool SendDeleteTestOwner(TTestActorRuntime &runtime, ui64 hiveTablet,
                              THolder<TEvHive::TEvDeleteOwnerTablets> ev, ui32 nodeIndex = 0,
                              NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK) {
        ui64 owner = ev->Record.GetOwner();
        ui64 txId = ev->Record.GetTxId();

        bool seenEvDeleteTabletResult = false;
        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletBase::EvDeleteTabletResult) {
                seenEvDeleteTabletResult = true;
            }
            return prevObserverFunc(runtime, event);
        });
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, ev.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto deleteTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvDeleteOwnerTabletsReply>(handle);
        UNIT_ASSERT(deleteTabletReply);
        UNIT_ASSERT_EQUAL_C(deleteTabletReply->Record.GetStatus(), expectedStatus,
                            (ui32)deleteTabletReply->Record.GetStatus() << " != " << (ui32)expectedStatus);
        UNIT_ASSERT_EQUAL_C(deleteTabletReply->Record.GetOwner(), owner,
                            deleteTabletReply->Record.GetOwner() << " != " << owner);
        UNIT_ASSERT_EQUAL_C(deleteTabletReply->Record.GetTxId(), txId,
                            deleteTabletReply->Record.GetTxId() << " != " << txId);
        runtime.SetObserverFunc(prevObserverFunc);
        return seenEvDeleteTabletResult;
    }

    void WaitEvDeleteTabletResult(TTestActorRuntime& runtime) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletBase::EvDeleteTabletResult);
        runtime.DispatchEvents(options);
    }

    void SendStopTablet(TTestActorRuntime &runtime, ui64 hiveTablet, ui64 tabletId, ui32 nodeIndex) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvStopTablet(tabletId), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto stopTabletResult = runtime.GrabEdgeEventRethrow<TEvHive::TEvStopTabletResult>(handle);
        UNIT_ASSERT(stopTabletResult);
        UNIT_ASSERT_EQUAL_C(stopTabletResult->Record.GetTabletID(), tabletId,
            stopTabletResult->Record.GetTabletID() << " != " << tabletId);
        UNIT_ASSERT_EQUAL_C(stopTabletResult->Record.GetStatus(), NKikimrProto::OK,
            (ui32)stopTabletResult->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
    }

    void SendReassignTablet(TTestActorRuntime &runtime,
                            ui64 hiveTablet,
                            ui64 tabletId,
                            const TVector<ui32>& channels,
                            ui32 nodeIndex) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvReassignTablet(tabletId, channels), 0, GetPipeConfigWithRetries());
    }

    void SendReassignTabletSpace(TTestActorRuntime &runtime,
                                 ui64 hiveTablet,
                                 ui64 tabletId,
                                 const TVector<ui32>& channels,
                                 ui32 nodeIndex) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvReassignTabletSpace(tabletId, channels), 0, GetPipeConfigWithRetries());
    }

    void MakeSureTabletIsDown(TTestActorRuntime &runtime, ui64 tabletId, ui32 nodeIndex) {
        TActorId sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.ConnectToPipe(tabletId, sender, nodeIndex, NTabletPipe::TClientConfig());
        bool isException = false;
        TEvTabletPipe::TEvClientConnected* clientConnectedResult;
        TAutoPtr<IEventHandle> handle;
        try {
            do {
                clientConnectedResult = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(handle);
            } while(handle->Recipient != sender);
        } catch (...) {
            isException = true;
        }
        UNIT_ASSERT(isException || clientConnectedResult->Status != NKikimrProto::OK);
        runtime.ResetScheduledCount();
    }

    void CreateLocal(TTestActorRuntime &runtime, ui32 nodeIndex, TLocalConfig::TPtr localConfig = {}) {
        if (localConfig == nullptr) {
            localConfig = new TLocalConfig();
            localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet,
                TMailboxType::Simple, 0,
                TMailboxType::Simple, 0);
        }
        TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
        tenantPoolConfig->AddStaticSlot(DOMAIN_NAME);

        TActorId actorId = runtime.Register(
            CreateTenantPool(tenantPoolConfig), nodeIndex, 0, TMailboxType::Revolving, 0);
        runtime.EnableScheduleForActor(actorId, true);
        runtime.RegisterService(MakeTenantPoolRootID(), actorId, nodeIndex);
    }

    void CreateLocalForTenant(TTestActorRuntime &runtime, ui32 nodeIndex, const TString& tenant) {
        TLocalConfig::TPtr localConfig(new TLocalConfig());
        localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(
                    &CreateFlatDummyTablet,
                    TMailboxType::Simple, 0,
                    TMailboxType::Simple, 0);
        localConfig->TabletClassInfo[TTabletTypes::Hive].SetupInfo = new TTabletSetupInfo(
                    &CreateDefaultHive,
                    TMailboxType::Simple, 0,
                    TMailboxType::Simple, 0);
        TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
        tenantPoolConfig->AddStaticSlot(tenant);

        TActorId actorId = runtime.Register(
            CreateTenantPool(tenantPoolConfig), nodeIndex, 0, TMailboxType::Revolving, 0);
        runtime.EnableScheduleForActor(actorId, true);
        runtime.RegisterService(MakeTenantPoolID(runtime.GetNodeId(nodeIndex)), actorId, nodeIndex);
    }

    void MakeSureTabletIsUp(TTestActorRuntime &runtime, ui64 tabletId, ui32 nodeIndex, NTabletPipe::TClientConfig* pipeConfig = nullptr, bool* roleConnected = nullptr) {
        TActorId sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
        for(;;) {
            TAutoPtr<IEventHandle> handle;
            auto clientConnectedResult = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(handle);
            if (handle->Recipient == sender) {
                UNIT_ASSERT(clientConnectedResult->Status == NKikimrProto::OK);
                if (roleConnected != nullptr) {
                    *roleConnected = clientConnectedResult->Leader;
                }
                break;
            }
        }
    }

    void MakeSureTheTabletIsDeleted(TTestActorRuntime &runtime, ui64 hiveTablet, ui64 tabletId) {
        TActorId sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            UNIT_ASSERT_VALUES_UNEQUAL(tablet.GetTabletID(), tabletId);
        }
    }

    void WaitForTabletIsUp(
                TTestActorRuntime &runtime,
                i64 tabletId,
                ui32 nodeIndex,
                NTabletPipe::TClientConfig* pipeConfig = nullptr,
                bool* roleConnected = nullptr,
                ui32 maxAttempts = 10) {
        TActorId sender = runtime.AllocateEdgeActor(nodeIndex);
        ui32 attempts = 0;
        runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
        for(;;) {
            TAutoPtr<IEventHandle> handle;
            auto result = runtime.GrabEdgeEventsRethrow<TEvTabletPipe::TEvClientConnected, TEvTabletPipe::TEvClientDestroyed>(handle);
            if (handle->Recipient == sender) {
                if (std::get<TEvTabletPipe::TEvClientDestroyed*>(result) != nullptr) {
                    UNIT_ASSERT(++attempts < maxAttempts);
                    runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
                    continue;
                }
                TEvTabletPipe::TEvClientConnected* event = std::get<TEvTabletPipe::TEvClientConnected*>(result);
                UNIT_ASSERT(event != nullptr);
                UNIT_ASSERT(event->Type() == TEvTabletPipe::TEvClientConnected::EventType);
                UNIT_ASSERT(event->Status == NKikimrProto::OK);
                if (roleConnected != nullptr) {
                    *roleConnected = event->Leader;
                }
                break;
            }
        }
    }

    bool CheckTabletIsUp(
                TTestActorRuntime &runtime,
                i64 tabletId,
                ui32 nodeIndex,
                NTabletPipe::TClientConfig* pipeConfig = nullptr,
                bool* roleConnected = nullptr,
                ui32 maxAttempts = 10) {
        TActorId sender = runtime.AllocateEdgeActor(nodeIndex);
        ui32 attempts = 0;
        runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
        for(;;) {
            TAutoPtr<IEventHandle> handle;
            auto result = runtime.GrabEdgeEventsRethrow<TEvTabletPipe::TEvClientConnected, TEvTabletPipe::TEvClientDestroyed>(handle);
            if (handle->Recipient == sender) {
                if (std::get<TEvTabletPipe::TEvClientDestroyed*>(result) != nullptr) {
                    if (++attempts >= maxAttempts) {
                        Ctest << "Couldn't establish pipe because of TEvClientDestroyed" << Endl;
                        return false;
                    }
                    runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
                    continue;
                }
                TEvTabletPipe::TEvClientConnected* event = std::get<TEvTabletPipe::TEvClientConnected*>(result);
                if ((event != nullptr)
                        && (event->Type() == TEvTabletPipe::TEvClientConnected::EventType)
                        && (event->Status == NKikimrProto::OK)) {
                    if (roleConnected != nullptr) {
                        *roleConnected = event->Leader;
                    }
                    return true;
                } else {
                    if ((event != nullptr)
                            && (event->Type() == TEvTabletPipe::TEvClientConnected::EventType)
                            && (event->Status == NKikimrProto::TRYLATER || event->Status == NKikimrProto::ERROR)) {
                        if (++attempts >= maxAttempts) {
                            Ctest << "Couldn't establish pipe because of status " << event->Status << Endl;
                            return false;
                        }
                        runtime.ConnectToPipe(tabletId, sender, nodeIndex, pipeConfig ? *pipeConfig : GetPipeConfigWithRetries());
                        continue;
                    }
                    return false;
                }
            }
        }
    }

    static bool TabletActiveEvent(IEventHandle& ev) {
        if (ev.GetTypeRewrite() == NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
            if (ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>()->Record.GetState()
                    == NKikimrWhiteboard::TTabletStateInfo::Active) {
                return true;
            }
        }
        /*if (ev.GetTypeRewrite() == TEvLocal::TEvTabletStatus::EventType) {
            if (ev.Get<TEvLocal::TEvTabletStatus>()->Record.GetStatus() == TEvLocal::TEvTabletStatus::StatusOk) {
                return true;
            }
        }*/
        return false;
    }

    void WaitForTabletsBecomeActive(TTestActorRuntime& runtime, ui32 count) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(&NTestSuiteTHiveTest::TabletActiveEvent, count);
        runtime.DispatchEvents(options);
    }

    NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestBasicRuntime& runtime, ui64 tabletId) {
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters);
        auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(sender);

        UNIT_ASSERT(ev);
        return ev->Get()->Record;
    }

    ui64 GetSimpleCounter(TTestBasicRuntime& runtime, ui64 tabletId, const TString& name) {
        const auto counters = GetCounters(runtime, tabletId);
        for (const auto& counter : counters.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (name != counter.GetName()) {
                continue;
            }

            return counter.GetValue();
        }

        UNIT_ASSERT_C(false, "Counter not found: " << name);
        return 0; // unreachable
    }

    void WaitForBootQueue(TTestBasicRuntime& runtime, ui64 hiveTabletId) {
        for (;;) {
            auto counters = GetCounters(runtime, hiveTabletId);
            ui64 bootQueueSize = counters.GetTabletCounters().GetAppCounters().GetSimpleCounters(NHive::COUNTER_BOOTQUEUE_SIZE).GetValue();
            ui64 waitQueueSize = counters.GetTabletCounters().GetAppCounters().GetSimpleCounters(NHive::COUNTER_WAITQUEUE_SIZE).GetValue();
            Ctest << "Hive/BootQueueSize=" << bootQueueSize << Endl;
            Ctest << "Hive/WaitQueueSize=" << bootQueueSize << Endl;
            if (bootQueueSize == 0 && waitQueueSize == 0) {
                break;
            }
            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(500));
        }
    }

    Y_UNIT_TEST(TestCreateTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        MakeSureTabletIsUp(runtime, hiveTablet, 0);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestBlockCreateTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        MakeSureTabletIsUp(runtime, hiveTablet, 0);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        THolder<TEvHive::TEvDeleteOwnerTablets> deleteOwner = MakeHolder<TEvHive::TEvDeleteOwnerTablets>(testerTablet, 1);
        TActorId senderB = runtime.AllocateEdgeActor(0);
        runtime.SendToPipe(hiveTablet, senderB, deleteOwner.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto deleteTabletsReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvDeleteOwnerTabletsReply>(handle);
        UNIT_ASSERT(deleteTabletsReply);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvCreateTablet(testerTablet, 1, tabletType, BINDED_CHANNELS), 0, GetPipeConfigWithRetries());
        auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::BLOCKED,
            createTabletReply->Record.GetStatus() << " != " << NKikimrProto::BLOCKED);
    }

    Y_UNIT_TEST(TestCreate100Tablets) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        MakeSureTabletIsUp(runtime, hiveTablet, 0);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<TTabletId> tablets;
        TActorId senderB = runtime.AllocateEdgeActor(0);
        for (int i = 0; i < 100; ++i) {
            runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvCreateTablet(testerTablet, i, tabletType, BINDED_CHANNELS), 0, GetPipeConfigWithRetries());
        }
        for (int i = 0; i < 100; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            ui64 tabletId = createTabletReply->Record.GetTabletID();
            tablets.emplace_back(tabletId);
        }
        for (TTabletId tabletId : tablets) {
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }
    }

    Y_UNIT_TEST(TestDrain) {
        const int NUM_NODES = 3;
        const int NUM_TABLETS = 100;
        TTestBasicRuntime runtime(NUM_NODES, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        std::unordered_set<TTabletId> tablets;
        TActorId senderA = runtime.AllocateEdgeActor(0);
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            auto* followerGroup = ev->Record.AddFollowerGroups();
            followerGroup->SetFollowerCount(1);
            runtime.SendToPipe(hiveTablet, senderA, ev.Release(), 0, GetPipeConfigWithRetries());
        }
        for (int i = 0; i < NUM_TABLETS; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            ui64 tabletId = createTabletReply->Record.GetTabletID();
            tablets.emplace(tabletId);
        }
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceFollower = true;
        for (TTabletId tabletId : tablets) {
            MakeSureTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        }

        ui32 nodeId = runtime.GetNodeId(0);
        int drainMovements = 0;
        {
            runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvDrainNode(nodeId));
            TAutoPtr<IEventHandle> handle;
            auto drainResponse = runtime.GrabEdgeEventRethrow<TEvHive::TEvDrainNodeResult>(handle, TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(drainResponse->Record.GetStatus(), NKikimrProto::EReplyStatus::OK);
            drainMovements = drainResponse->Record.GetMovements();
            UNIT_ASSERT(drainMovements > 0);
        }

        std::unordered_map<NKikimrWhiteboard::TTabletStateInfo::ETabletState, int> tabletStates;
        {
            TAutoPtr<IEventHandle> handle;
            TActorId whiteboard = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
            runtime.Send(new IEventHandle(whiteboard, senderA, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest()));
            NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse* wbResponse = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse>(handle);
            for (const NKikimrWhiteboard::TTabletStateInfo& tabletInfo : wbResponse->Record.GetTabletStateInfo()) {
                if (tablets.count(tabletInfo.GetTabletId()) == 0) {
                    continue;
                }
                tabletStates[tabletInfo.GetState()]++;
                if (tabletInfo.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                    Ctest << "Tablet " << tabletInfo.GetTabletId() << "." << tabletInfo.GetFollowerId()
                        << " is not dead yet (" << NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(tabletInfo.GetState()) << ")" << Endl;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(tabletStates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tabletStates[NKikimrWhiteboard::TTabletStateInfo::Dead], drainMovements);
    }

    Y_UNIT_TEST(TestCreateSubHiveCreateTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
        MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
        MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good

        TActorId sender = runtime.AllocateEdgeActor(0);
        InitSchemeRoot(runtime, sender);

        TSubDomainKey subdomainKey;

        // Create subdomain
        do {
            auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            auto* tran = x->Record.AddTransaction();
            tran->SetWorkingDir("/dc-1");
            tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
            auto* subd = tran->MutableSubDomain();
            subd->SetName("tenant1");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
        createHive->Record.AddAllowedDomains();
        createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
            }
            return prevObserverFunc(runtime, event);
        });

        SendToLocal(runtime, 0, new TEvLocal::TEvAddTenant("/dc-1/tenant1"));

        MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

        THolder<TEvHive::TEvCreateTablet> createTablet = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet->Record.AddAllowedDomains();
        createTablet->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createTablet->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 tabletId = SendCreateTestTablet(runtime, subHiveTablet, testerTablet, std::move(createTablet), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0); // dummy from sub hive also good
        runtime.SetObserverFunc(prevObserverFunc);
    }

    Y_UNIT_TEST(TestCheckSubHiveForwarding) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
        MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
        MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good


        TActorId sender = runtime.AllocateEdgeActor(0);
        InitSchemeRoot(runtime, sender);

        TSubDomainKey subdomainKey;
        // Create subdomain
        do {
            auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            auto* tran = x->Record.AddTransaction();
            tran->SetWorkingDir("/dc-1");
            tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
            auto* subd = tran->MutableSubDomain();
            subd->SetName("tenant1");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
        createHive->Record.AddAllowedDomains();
        createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
            }
            return prevObserverFunc(runtime, event);
        });

        SendToLocal(runtime, 0, new TEvLocal::TEvAddTenant("/dc-1/tenant1"));

        MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

        THolder<TEvHive::TEvCreateTablet> createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet1->Record.AddAllowedDomains();
        createTablet1->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createTablet1->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 tabletId1 = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createTablet1), 0, true);
        MakeSureTabletIsUp(runtime, tabletId1, 0);

        THolder<TEvHive::TEvCreateTablet> createTablet2 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 2, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet2->Record.AddAllowedDomains();
        createTablet2->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createTablet2->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 tabletId2 = SendCreateTestTablet(runtime, subHiveTablet, testerTablet, std::move(createTablet2), 0, true);
        MakeSureTabletIsUp(runtime, tabletId2, 0); // dummy from sub hive also good

        // retry create request to sub domain hive
        createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet1->Record.SetTabletID(tabletId1);

        runtime.SendToPipe(subHiveTablet, sender, createTablet1.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT(createTabletReply->Record.HasForwardRequest());
        UNIT_ASSERT_VALUES_EQUAL(createTabletReply->Record.GetForwardRequest().GetHiveTabletId(), hiveTablet);

        // trying to delete same tablet from sub domain hive
        THolder<TEvHive::TEvDeleteTablet> deleteTablet1 = MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 1, 0);
        deleteTablet1->Record.AddTabletID(tabletId1);

        runtime.SendToPipe(subHiveTablet, sender, deleteTablet1.Release(), 0, GetPipeConfigWithRetries());
        auto deleteTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvDeleteTabletReply>(handle);
        UNIT_ASSERT(deleteTabletReply);
        UNIT_ASSERT(deleteTabletReply->Record.HasForwardRequest());
        UNIT_ASSERT_VALUES_EQUAL(deleteTabletReply->Record.GetForwardRequest().GetHiveTabletId(), hiveTablet);

        // retry create request to root hive
        createTablet2 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 2, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet2->Record.SetTabletID(tabletId2);

        runtime.SendToPipe(hiveTablet, sender, createTablet2.Release(), 0, GetPipeConfigWithRetries());
        createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT(createTabletReply->Record.HasForwardRequest());
        UNIT_ASSERT_VALUES_EQUAL(createTabletReply->Record.GetForwardRequest().GetHiveTabletId(), subHiveTablet);

        // trying to delete same tablet from root hive
        THolder<TEvHive::TEvDeleteTablet> deleteTablet2 = MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 2, 0);
        deleteTablet2->Record.AddTabletID(tabletId2);

        runtime.SendToPipe(hiveTablet, sender, deleteTablet2.Release(), 0, GetPipeConfigWithRetries());
        deleteTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvDeleteTabletReply>(handle);
        UNIT_ASSERT(deleteTabletReply);
        UNIT_ASSERT(deleteTabletReply->Record.HasForwardRequest());
        UNIT_ASSERT_VALUES_EQUAL(deleteTabletReply->Record.GetForwardRequest().GetHiveTabletId(), subHiveTablet);

        runtime.SetObserverFunc(prevObserverFunc);
    }

    Y_UNIT_TEST(TestCheckSubHiveMigration) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, true);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
        MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
        MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good


        TActorId sender = runtime.AllocateEdgeActor(0);
        InitSchemeRoot(runtime, sender);

        TSubDomainKey subdomainKey;

        // Create subdomain
        do {
            auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            auto* tran = x->Record.AddTransaction();
            tran->SetWorkingDir("/dc-1");
            tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
            auto* subd = tran->MutableSubDomain();
            subd->SetName("tenant1");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
        createHive->Record.AddAllowedDomains();
        createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
            }
            return prevObserverFunc(runtime, event);
        });

        SendKillLocal(runtime, 1);
        CreateLocalForTenant(runtime, 1, "/dc-1/tenant1");

        MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

        THolder<TEvHive::TEvConfigureHive> configureHive = MakeHolder<TEvHive::TEvConfigureHive>(subdomainKey);

        runtime.SendToPipe(subHiveTablet, sender, configureHive.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;

        auto configureHiveReply = runtime.GrabEdgeEventRethrow<TEvSubDomain::TEvConfigureStatus>(handle);

        Y_UNUSED(configureHiveReply);

        THolder<TEvHive::TEvCreateTablet> createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet1->Record.AddAllowedDomains();
        createTablet1->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createTablet1->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 tabletId1 = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createTablet1), 0, true);

        MakeSureTabletIsUp(runtime, tabletId1, 0); // tablet up in root hive

        int iterations = 0;

        for (;; ++iterations) {
            UNIT_ASSERT(iterations < 100); // 10 seconds max

            runtime.SendToPipe(subHiveTablet, sender, new TEvHive::TEvQueryMigration(), 0, GetPipeConfigWithRetries());
            auto queryMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvQueryMigrationReply>(handle);

            if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_COMPLETE) {
                break;
            }

            if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_READY) {
                THolder<TEvHive::TEvInitMigration> migration = MakeHolder<TEvHive::TEvInitMigration>();
                runtime.SendToPipe(subHiveTablet, sender, migration.Release(), 0, GetPipeConfigWithRetries());
                auto initMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvInitMigrationReply>(handle);
                UNIT_ASSERT(initMigrationReply);
                UNIT_ASSERT(initMigrationReply->Record.GetStatus() == NKikimrProto::OK);
            }

            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        }

        MakeSureTabletIsUp(runtime, tabletId1, 0); // tablet up in sub hive

        // retry create request to sub domain hive
        createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
        createTablet1->Record.SetTabletID(tabletId1);

        runtime.SendToPipe(hiveTablet, sender, createTablet1.Release(), 0, GetPipeConfigWithRetries());
        auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT(createTabletReply->Record.HasForwardRequest());
        UNIT_ASSERT_VALUES_EQUAL(createTabletReply->Record.GetForwardRequest().GetHiveTabletId(), subHiveTablet);

        runtime.SetObserverFunc(prevObserverFunc);
    }

    Y_UNIT_TEST(TestCheckSubHiveMigrationManyTablets) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, true);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
        MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
        MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good


        TActorId sender = runtime.AllocateEdgeActor(0);
        InitSchemeRoot(runtime, sender);

        TSubDomainKey subdomainKey;

        // Create subdomain
        do {
            auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            auto* tran = x->Record.AddTransaction();
            tran->SetWorkingDir("/dc-1");
            tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
            auto* subd = tran->MutableSubDomain();
            subd->SetName("tenant1");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
        createHive->Record.AddAllowedDomains();
        createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
            }
            return prevObserverFunc(runtime, event);
        });

        SendKillLocal(runtime, 1);
        CreateLocalForTenant(runtime, 1, "/dc-1/tenant1");

        MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

        THolder<TEvHive::TEvConfigureHive> configureHive = MakeHolder<TEvHive::TEvConfigureHive>(subdomainKey);

        runtime.SendToPipe(subHiveTablet, sender, configureHive.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;

        auto configureHiveReply = runtime.GrabEdgeEventRethrow<TEvSubDomain::TEvConfigureStatus>(handle);

        Y_UNUSED(configureHiveReply);

        static constexpr int TABLETS = 100;

        std::vector<ui64> tabletIds;

        for (int i = 0; i < TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, i + 1, TTabletTypes::Dummy, BINDED_CHANNELS);
            createTablet1->Record.AddAllowedDomains();
            createTablet1->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
            createTablet1->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
            ui64 tabletId1 = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createTablet1), 0, true);
            MakeSureTabletIsUp(runtime, tabletId1, 0); // tablet up in root hive
            tabletIds.push_back(tabletId1);
        }

        int iterations = 0;

        for (;; ++iterations) {
            UNIT_ASSERT(iterations < 300); // 30 seconds max

            runtime.SendToPipe(subHiveTablet, sender, new TEvHive::TEvQueryMigration(), 0, GetPipeConfigWithRetries());
            auto queryMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvQueryMigrationReply>(handle);

            if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_COMPLETE) {
                break;
            }

            if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_READY) {
                THolder<TEvHive::TEvInitMigration> migration = MakeHolder<TEvHive::TEvInitMigration>();
                runtime.SendToPipe(subHiveTablet, sender, migration.Release(), 0, GetPipeConfigWithRetries());
                auto initMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvInitMigrationReply>(handle);
                UNIT_ASSERT(initMigrationReply);
                UNIT_ASSERT(initMigrationReply->Record.GetStatus() == NKikimrProto::OK);
            }

            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        }

        for (int i = 0; i < (int)tabletIds.size(); ++i) {
            MakeSureTabletIsUp(runtime, tabletIds[i], 0); // tablet up in sub hive

            // retry create request to sub domain hive
            THolder<TEvHive::TEvCreateTablet> createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, i + 1, TTabletTypes::Dummy, BINDED_CHANNELS);
            createTablet1->Record.SetTabletID(tabletIds[i]);

            runtime.SendToPipe(hiveTablet, sender, createTablet1.Release(), 0, GetPipeConfigWithRetries());
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT(createTabletReply->Record.HasForwardRequest());
            UNIT_ASSERT_VALUES_EQUAL(createTabletReply->Record.GetForwardRequest().GetHiveTabletId(), subHiveTablet);
        }

        runtime.SetObserverFunc(prevObserverFunc);
    }

    Y_UNIT_TEST(TestCreateSubHiveCreateManyTablets) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        static constexpr int TABLETS = 1000;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
        MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
        MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good

        TActorId sender = runtime.AllocateEdgeActor(0);
        InitSchemeRoot(runtime, sender);

        TSubDomainKey subdomainKey;

        // Create subdomain
        do {
            auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
            auto* tran = x->Record.AddTransaction();
            tran->SetWorkingDir("/dc-1");
            tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
            auto* subd = tran->MutableSubDomain();
            subd->SetName("tenant1");
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
            if (reply) {
                subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                break;
            }
        } while (true);

        THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
        createHive->Record.AddAllowedDomains();
        createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
            }
            return prevObserverFunc(runtime, event);
        });

        SendToLocal(runtime, 0, new TEvLocal::TEvAddTenant("/dc-1/tenant1"));

        MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

        NKikimrHive::TEvCreateTablet templateCreateTablet;
        templateCreateTablet.SetOwner(testerTablet);
        templateCreateTablet.SetOwnerIdx(0);
        templateCreateTablet.SetTabletType(TTabletTypes::Dummy);
        for (auto& channel : BINDED_CHANNELS) {
            (*templateCreateTablet.AddBindedChannels()) = channel;
        }
        templateCreateTablet.AddAllowedDomains();
        templateCreateTablet.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
        templateCreateTablet.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
        for (int ownerIdx = 1; ownerIdx <= TABLETS; ++ownerIdx) {
            THolder<TEvHive::TEvCreateTablet> createTablet = MakeHolder<TEvHive::TEvCreateTablet>();
            createTablet->Record = templateCreateTablet;
            createTablet->Record.SetOwnerIdx(ownerIdx);
            runtime.SendToPipe(subHiveTablet, sender, createTablet.Release(), 0, GetPipeConfigWithRetries());
        }

        for (int ownerIdx = 1; ownerIdx <= TABLETS; ++ownerIdx) {
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            ui64 tabletId = createTabletReply->Record.GetTabletID();
            MakeSureTabletIsUp(runtime, tabletId, 0); // dummy from sub hive also good
        }

        runtime.SetObserverFunc(prevObserverFunc);
    }

    Y_UNIT_TEST(TestCreateSubHiveCreateManyTabletsWithReboots) {
        static constexpr int TABLETS = 100;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        THiveInitialEventsFilter initialEventsFilter;

        TVector<ui64> tabletIds;
        tabletIds.push_back(hiveTablet);
        tabletIds.push_back(testerTablet);
        RunTestWithReboots(tabletIds, [&]() {
            return initialEventsFilter.Prepare();
        }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
            if (ENABLE_DETAILED_HIVE_LOG) {
                Ctest << "At dispatch " << dispatchName << Endl;
            }
            TTestBasicRuntime runtime(2, false);
            Setup(runtime, true);
            setup(runtime);

            CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
            CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
            MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
            MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good

            TActorId sender = runtime.AllocateEdgeActor(0);
            InitSchemeRoot(runtime, sender);

            TSubDomainKey subdomainKey;

            // Create subdomain
            do {
                auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
                auto* tran = x->Record.AddTransaction();
                tran->SetWorkingDir("/dc-1");
                tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
                auto* subd = tran->MutableSubDomain();
                subd->SetName("tenant1");
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
                TAutoPtr<IEventHandle> handle;
                auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
                if (reply) {
                    subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                    break;
                }
            } while (true);

            THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
            createHive->Record.AddAllowedDomains();
            createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
            createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
            ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

            TTestActorRuntime::TEventObserver prevObserverFunc;
            prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                    event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                    MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
                }
                return prevObserverFunc(runtime, event);
            });

            SendKillLocal(runtime, 1);
            CreateLocalForTenant(runtime, 1, "/dc-1/tenant1");

            MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

            activeZone = true;

            NKikimrHive::TEvCreateTablet templateCreateTablet;
            templateCreateTablet.SetOwner(testerTablet);
            templateCreateTablet.SetOwnerIdx(0);
            templateCreateTablet.SetTabletType(TTabletTypes::Dummy);
            for (auto& channel : BINDED_CHANNELS) {
                (*templateCreateTablet.AddBindedChannels()) = channel;
            }
            templateCreateTablet.AddAllowedDomains();
            templateCreateTablet.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
            templateCreateTablet.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
            for (int ownerIdx = 1; ownerIdx <= TABLETS; ++ownerIdx) {
                THolder<TEvHive::TEvCreateTablet> createTablet = MakeHolder<TEvHive::TEvCreateTablet>();
                createTablet->Record = templateCreateTablet;
                createTablet->Record.SetOwnerIdx(ownerIdx);
                runtime.SendToPipe(subHiveTablet, sender, createTablet.Release(), 0, GetPipeConfigWithRetries());
            }

            for (int ownerIdx = 1; ownerIdx <= TABLETS; ++ownerIdx) {
                TAutoPtr<IEventHandle> handle;
                auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
                ui64 tabletId = createTabletReply->Record.GetTabletID();
                MakeSureTabletIsUp(runtime, tabletId, 0); // dummy from sub hive also good
            }

            activeZone = false;

            runtime.SetObserverFunc(prevObserverFunc);
        });
    }

    Y_UNIT_TEST(TestCheckSubHiveMigrationWithReboots) {
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 bsControllerTablet = MakeBSControllerID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        THiveEveryEventFilter everyEventFilter;

        TVector<ui64> tabletIds;
        tabletIds.push_back(hiveTablet);
        tabletIds.push_back(bsControllerTablet);
        tabletIds.push_back(65536); // sub hive
        tabletIds.push_back(testerTablet);
        RunTestWithReboots(tabletIds, [&]() {
            return everyEventFilter.Prepare();
        }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
            if (ENABLE_DETAILED_HIVE_LOG) {
                Ctest << "At dispatch " << dispatchName << Endl;
            }
            TTestBasicRuntime runtime(2, false);
            Setup(runtime, true);
            setup(runtime);

            CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
            CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::SchemeShard, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
            MakeSureTabletIsUp(runtime, hiveTablet, 0); // root hive good
            MakeSureTabletIsUp(runtime, TTestTxConfig::SchemeShard, 0); // root ss good

            TActorId sender = runtime.AllocateEdgeActor(0);
            InitSchemeRoot(runtime, sender);

            TSubDomainKey subdomainKey;

            // Create subdomain
            do {
                auto x = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
                auto* tran = x->Record.AddTransaction();
                tran->SetWorkingDir("/dc-1");
                tran->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
                auto* subd = tran->MutableSubDomain();
                subd->SetName("tenant1");
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, x.Release());
                TAutoPtr<IEventHandle> handle;
                auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle, TDuration::MilliSeconds(100));
                if (reply) {
                    subdomainKey = TSubDomainKey(reply->Record.GetSchemeshardId(), reply->Record.GetPathId());
                    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                    break;
                }
            } while (true);

            THolder<TEvHive::TEvCreateTablet> createHive = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, TTabletTypes::Hive, BINDED_CHANNELS);
            createHive->Record.AddAllowedDomains();
            createHive->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
            createHive->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
            ui64 subHiveTablet = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createHive), 0, false);

            TTestActorRuntime::TEventObserver prevObserverFunc;
            prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult) {
                    event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord()->
                    MutablePathDescription()->MutableDomainDescription()->MutableProcessingParams()->SetHive(subHiveTablet);
                }
                return prevObserverFunc(runtime, event);
            });

            SendKillLocal(runtime, 1);
            CreateLocalForTenant(runtime, 1, "/dc-1/tenant1");

            MakeSureTabletIsUp(runtime, subHiveTablet, 0); // sub hive good

            runtime.SetObserverFunc(prevObserverFunc);

            THolder<TEvHive::TEvConfigureHive> configureHive = MakeHolder<TEvHive::TEvConfigureHive>(subdomainKey);

            runtime.SendToPipe(subHiveTablet, sender, configureHive.Release(), 0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;

            auto configureHiveReply = runtime.GrabEdgeEventRethrow<TEvSubDomain::TEvConfigureStatus>(handle);

            Y_UNUSED(configureHiveReply);

            THolder<TEvHive::TEvCreateTablet> createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
            createTablet1->Record.AddAllowedDomains();
            createTablet1->Record.MutableAllowedDomains(0)->SetSchemeShard(subdomainKey.first);
            createTablet1->Record.MutableAllowedDomains(0)->SetPathId(subdomainKey.second);
            ui64 tabletId1 = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(createTablet1), 0, true);

            MakeSureTabletIsUp(runtime, tabletId1, 0); // tablet up in root hive

            activeZone = true;

            int iterations = 0;

            for (;; ++iterations) {
                UNIT_ASSERT(iterations < 100); // 10 seconds max

                runtime.SendToPipe(subHiveTablet, sender, new TEvHive::TEvQueryMigration(), 0, GetPipeConfigWithRetries());
                auto queryMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvQueryMigrationReply>(handle, TDuration::MilliSeconds(100));

                if (queryMigrationReply) {
                    if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_COMPLETE) {
                        break;
                    }

                    if (queryMigrationReply->Record.GetMigrationState() == NKikimrHive::EMigrationState::MIGRATION_READY) {
                        // restart migration when needed
                        THolder<TEvHive::TEvInitMigration> migration = MakeHolder<TEvHive::TEvInitMigration>();
                        runtime.SendToPipe(subHiveTablet, sender, migration.Release(), 0, GetPipeConfigWithRetries());
                        auto initMigrationReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvInitMigrationReply>(handle, TDuration::MilliSeconds(100));
                        if (initMigrationReply) {
                            UNIT_ASSERT(initMigrationReply->Record.GetStatus() == NKikimrProto::OK);
                        }
                    }

                    TDispatchOptions options;
                    runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
                }
            }

            activeZone = false;

            MakeSureTabletIsUp(runtime, tabletId1, 0); // tablet up in sub hive

            // retry create request to sub domain hive
            createTablet1 = MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 1, TTabletTypes::Dummy, BINDED_CHANNELS);
            createTablet1->Record.SetTabletID(tabletId1);

            runtime.SendToPipe(hiveTablet, sender, createTablet1.Release(), 0, GetPipeConfigWithRetries());

            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT(createTabletReply->Record.GetStatus() == NKikimrProto::INVALID_OWNER);
            UNIT_ASSERT(createTabletReply->Record.HasForwardRequest());
            UNIT_ASSERT_VALUES_EQUAL(createTabletReply->Record.GetForwardRequest().GetHiveTabletId(), subHiveTablet);

        });
    }

    Y_UNIT_TEST(TestCreateAndDeleteTabletWithStoragePoolsReboots) {
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 bsControllerTablet = MakeBSControllerID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);

        THiveInitialEventsFilter initialEventsFilter;

        TVector<ui64> tabletIds;
        tabletIds.push_back(hiveTablet);
        tabletIds.push_back(bsControllerTablet);
        tabletIds.push_back(testerTablet);
        RunTestWithReboots(tabletIds, [&]() {
            return initialEventsFilter.Prepare();
        }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
            if (ENABLE_DETAILED_HIVE_LOG) {
                Ctest << "At dispatch " << dispatchName << Endl;
            }
            TTestBasicRuntime runtime(1, false);
            Setup(runtime, true);
            setup(runtime);

            CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

            TAutoPtr<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, TTabletTypes::Dummy, BINDED_CHANNELS));
            const bool doWaitForResult = false;
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, ev, 0, doWaitForResult);

            activeZone = true;
            {
                bool allowIncompleteResult = (dispatchName != INITIAL_TEST_DISPATCH_NAME);
                try {
                    MakeSureTabletIsUp(runtime, tabletId, 0);
                } catch (TEmptyEventQueueException&) {
                    Ctest << "Event queue is empty at dispatch " << dispatchName << "\n";
                    if (!allowIncompleteResult)
                        throw;
                }
            }
            activeZone = false;
        });
    }

    Y_UNIT_TEST(TestCreateAndDeleteTabletWithStoragePools) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TAutoPtr<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        const bool doWaitForResult = true;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, ev, 0, doWaitForResult);

        MakeSureTabletIsUp(runtime, tabletId, 0);

        SendKillLocal(runtime, 0);
        WaitForEvServerDisconnected(runtime);

        MakeSureTabletIsDown(runtime, tabletId, 0);
        CreateLocal(runtime, 0);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        MakeSureTheTabletIsDeleted(runtime, hiveTablet, tabletId);
    }

    Y_UNIT_TEST(TestCreateAndReassignTabletWithStoragePools) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateLocal(runtime, 0);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TAutoPtr<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        const bool doWaitForResult = true;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, ev, 0, doWaitForResult);

        MakeSureTabletIsUp(runtime, tabletId, 0);

        runtime.Register(CreateTabletKiller(hiveTablet, runtime.GetNodeId(0)));

        MakeSureTabletIsUp(runtime, hiveTablet, 0);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerSelectGroupsResult));
            runtime.DispatchEvents(options);
        }
        MakeSureTabletIsUp(runtime, tabletId, 0);

        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        {
            TActorId sender = runtime.AllocateEdgeActor();
            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
            TAutoPtr<IEventHandle> handle;
            TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
            for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                UNIT_ASSERT_VALUES_UNEQUAL(tablet.GetTabletID(), tabletId);
            }
        }
    }

    Y_UNIT_TEST(TestCreateAndReassignTabletWhileStarting) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true, 2);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateLocal(runtime, 0);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TAutoPtr<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        const bool doWaitForResult = true;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, ev, 0, doWaitForResult);

        MakeSureTabletIsUp(runtime, tabletId, 0);

        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvTabletStatus));
            runtime.DispatchEvents(options);
        }

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvTabletStatus));
            runtime.DispatchEvents(options);
        }

        MakeSureTabletIsUp(runtime, tabletId, 0);

        TVector<THolder<IEventHandle>> blockedCommits;
        auto blockCommits = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTablet::TEvCommit::EventType: {
                    auto* msg = ev->Get<TEvTablet::TEvCommit>();
                    if (msg->TabletID == hiveTablet) {
                        Ctest << "blocked commit for tablet " << msg->TabletID << Endl;
                        blockedCommits.push_back(std::move(ev));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        Ctest << "killing tablet " << tabletId << Endl;
        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(0)));

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvDeadTabletAck));
            runtime.DispatchEvents(options);
        }

        SendReassignTabletSpace(runtime, hiveTablet, tabletId, {}, 0);

        Ctest << "blocking commits" << Endl;
        auto prevObserver = runtime.SetObserverFunc(blockCommits);

        auto waitFor = [&](const auto& condition, const TString& description) {
            while (!condition()) {
                Ctest << "waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
            }
        };

        waitFor([&]{ return blockedCommits.size() >= 2; }, "at least 2 blocked commits");

        Ctest << "restoring commits" << Endl;
        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : blockedCommits) {
            runtime.Send(ev.Release(), 0, true);
        }

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvBootTablet));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestCreateTabletsWithRaceForStoragePoolsKIKIMR_9659) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime);

        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        CreateLocal(runtime, 0);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tabletIds;
        TActorId senderB = runtime.AllocateEdgeActor(0);
        for (int i = 0; i < 2; ++i) {
            TChannelsBindings bindings;
            for (int n = 0; n <= i + 1; ++n) {
                bindings.push_back(GetChannelBind(STORAGE_POOL + ToString(n + 1)));
            }
            TAutoPtr<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, i, tabletType, bindings));
            runtime.SendToPipe(hiveTablet, senderB, ev.Release(), 0, GetPipeConfigWithRetries());
        }

        for (int i = 0; i < 2; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            ui64 tabletId = createTabletReply->Record.GetTabletID();
            tabletIds.push_back(tabletId);
        }

        for (ui64 tabletId : tabletIds) {
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        runtime.Register(CreateTabletKiller(hiveTablet, runtime.GetNodeId(0)));

        MakeSureTabletIsUp(runtime, hiveTablet, 0);

        for (ui64 tabletId : tabletIds) {
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }
    }

    Y_UNIT_TEST(TestDeleteTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        const ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, false);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvBootTablet);
            runtime.DispatchEvents(options);
        }

        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            UNIT_ASSERT_VALUES_UNEQUAL(tablet.GetTabletID(), tabletId);
        }
    }

    Y_UNIT_TEST(TestDeleteOwnerTablets) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        const ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, false);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvBootTablet);
            runtime.DispatchEvents(options);
        }

        if (!SendDeleteTestOwner(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteOwnerTablets>(testerTablet, 123))) {
            WaitEvDeleteTabletResult(runtime);
        }

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            UNIT_ASSERT_VALUES_UNEQUAL(tablet.GetTabletID(), tabletId);
        }

        SendDeleteTestOwner(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteOwnerTablets>(testerTablet, 124), 0, NKikimrProto::OK);
    }

    Y_UNIT_TEST(TestDeleteOwnerTabletsMany) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        const ui64 count = 100;
        TSet<ui64> tabletIds;
        for (ui64 i = 0; i < count; ++i) {
            const ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, i, tabletType, BINDED_CHANNELS), 0, false);
            tabletIds.insert(tabletId);
        }

        SendDeleteTestOwner(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteOwnerTablets>(testerTablet, 123));

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            UNIT_ASSERT(!tabletIds.contains(tablet.GetTabletID()));
        }

        SendDeleteTestOwner(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteOwnerTablets>(testerTablet, 124), 0, NKikimrProto::OK);
    }

    Y_UNIT_TEST(TestDeleteTabletWithFollowers) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(2);
        followerGroup->SetRequireDifferentNodes(true);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;

        MakeSureTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        MakeSureTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        MakeSureTabletIsUp(runtime, tabletId, 2, &pipeConfig);

        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 100500, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        SendKillLocal(runtime, 0);
        WaitForEvServerDisconnected(runtime);
        SendKillLocal(runtime, 1);
        WaitForEvServerDisconnected(runtime);
        SendKillLocal(runtime, 2);
        WaitForEvServerDisconnected(runtime);
    }

    Y_UNIT_TEST(PipeAlivenessOfDeadTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = 1;
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        const ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }
        MakeSureTabletIsDown(runtime, tabletId, 0);

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.CheckAliveness = true;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        runtime.Register(NTabletPipe::CreateClient(sender, tabletId, clientConfig));
        TAutoPtr<IEventHandle> handle;
        auto connectResult = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(handle);
        UNIT_ASSERT(connectResult);
        UNIT_ASSERT(connectResult->Dead == true);
    }

    Y_UNIT_TEST(TestCreateTabletBeforeLocal) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, false);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, GetPipeConfigWithRetries());

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(
            TEvBlobStorage::EvControllerSelectGroupsResult));
        runtime.DispatchEvents(options);

        TAutoPtr<IEventHandle> handle;
        auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
        UNIT_ASSERT(createTabletReply);
        UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::OK,
            (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
        UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
            createTabletReply->Record.GetOwner() << " != " << testerTablet);
        ui64 tabletId = createTabletReply->Record.GetTabletID();

        // Start local only when transaction is complete
        {
            TLocalConfig::TPtr localConfig(new TLocalConfig());
            localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet,
                    TMailboxType::Simple, 0,
                    TMailboxType::Simple, 0);
            TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
            tenantPoolConfig->AddStaticSlot(DOMAIN_NAME);

            TActorId actorId = runtime.Register(CreateTenantPool(tenantPoolConfig));
            runtime.EnableScheduleForActor(actorId, true);
            runtime.RegisterService(MakeTenantPoolRootID(), actorId);
        }

        MakeSureTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestReCreateTablet) {
        TTestBasicRuntime runtime;
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        {
            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS),
                0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::OK,
                (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
                createTabletReply->Record.GetOwner() << " != " << testerTablet);
            ui64 tabletId = createTabletReply->Record.GetTabletID();

            MakeSureTabletIsUp(runtime, tabletId, 0);
        }
        {
            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS),
                0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::OK,
                (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
                createTabletReply->Record.GetOwner()  << " != " << testerTablet);
            ui64 tabletId = createTabletReply->Record.GetTabletID();

            MakeSureTabletIsUp(runtime, tabletId, 0);
        }
    }

    Y_UNIT_TEST(TestReCreateTabletError) {
        TTestBasicRuntime runtime;
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui32 nodeIndex = 0;
        SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), nodeIndex, true);
        {
            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, TTabletTypes::TxAllocator, BINDED_CHANNELS),
                nodeIndex, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), NKikimrProto::ERROR,
                (ui32)event->Record.GetStatus() << " != " << (ui32)NKikimrProto::ERROR);
            UNIT_ASSERT_EQUAL_C(event->Record.GetOwner(), testerTablet,
                event->Record.GetOwner() << " != " << testerTablet);
        }
    }

    Y_UNIT_TEST(TestCreateTabletReboots) {
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 bsControllerTablet = MakeBSControllerID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);

        THiveInitialEventsFilter initialEventsFilter;

        TVector<ui64> tabletIds;
        tabletIds.push_back(hiveTablet);
        tabletIds.push_back(bsControllerTablet);
        tabletIds.push_back(testerTablet);
        RunTestWithReboots(tabletIds, [&]() {
            return initialEventsFilter.Prepare();
        }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
            if (ENABLE_DETAILED_HIVE_LOG) {
                Ctest << "At dispatch " << dispatchName << Endl;
            }
            TTestBasicRuntime runtime;
            Setup(runtime, true);
            setup(runtime);
            TActorId sender = runtime.AllocateEdgeActor();

            CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
            runtime.DispatchEvents(options);

            ui64 tabletId = 0;
            TTabletTypes::EType tabletType = TTabletTypes::Dummy;

            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::OK,
                (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
                createTabletReply->Record.GetOwner() << " != " << testerTablet);
            tabletId = createTabletReply->Record.GetTabletID();

            activeZone = true;
            {
                bool allowIncompleteResult = (dispatchName != INITIAL_TEST_DISPATCH_NAME);
                try {
                    MakeSureTabletIsUp(runtime, tabletId, 0);
                } catch (TEmptyEventQueueException&) {
                    Ctest << "Event queue is empty at dispatch " << dispatchName << "\n";
                    if (!allowIncompleteResult)
                        throw;
                }
            }
            activeZone = false;
        });
    }

    Y_UNIT_TEST(TestLocalDisconnect) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        ui32 nodeIndex = 0;
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 100500, tabletType, BINDED_CHANNELS), nodeIndex, true);
        MakeSureTabletIsUp(runtime, tabletId, nodeIndex);
        SendKillLocal(runtime, nodeIndex);
        WaitForEvServerDisconnected(runtime);
        CreateLocal(runtime, nodeIndex);
        MakeSureTabletIsUp(runtime, tabletId, nodeIndex);
    }

    Y_UNIT_TEST(TestNodeDisconnect) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        TActorId sender = runtime.AllocateEdgeActor();
        //TAutoPtr<ITabletScheduledEventsGuard> guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetFollowerCount(1);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        runtime.SendToPipe(hiveTablet, sender, new TEvInterconnect::TEvNodeDisconnected(runtime.GetNodeId(0)));
        //TActorId local = MakeLocalID(runtime.GetNodeId(0));
        //runtime.Send(new IEventHandle(local, sender, new TEvTabletPipe::TEvClientDestroyed(hiveTablet, TActorId(), TActorId())), 0);
        SendKillLocal(runtime, 0);
        runtime.Register(CreateTabletKiller(hiveTablet));
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvPing));
            runtime.DispatchEvents(options);
        }
        CreateLocal(runtime, 0);
        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
    }

    Y_UNIT_TEST(TestLocalReplacement) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // Kill local on node 1
        SendKillLocal(runtime, 1);
        // Create the tablet
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 100500, tabletType, BINDED_CHANNELS), 0, true);
        // Make sure the tablet is OK
        WaitForTabletIsUp(runtime, tabletId, 0);
        // Re-create the local on node 1
        CreateLocal(runtime, 1);
        // Kill both local and the tablet on node 0
        SendKillLocal(runtime, 0);
        // Wait
        WaitForEvServerDisconnected(runtime);
        // Tablet should have moved to node 1
        // Make sure the tablet is OK
        WaitForTabletIsUp(runtime, tabletId, 1);
    }

    Y_UNIT_TEST(TestHiveRestart) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // creating tablet
        ui32 nodeIndex = 0;
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 100500, tabletType, BINDED_CHANNELS), nodeIndex, true);
        MakeSureTabletIsUp(runtime, tabletId, nodeIndex);

        TActorId senderA = runtime.AllocateEdgeActor();

        // first check, aquiring generation
        runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo(tabletId));
        TAutoPtr<IEventHandle> handle1;
        TEvHive::TEvResponseHiveInfo* response1 = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Record.TabletsSize(), 1);
        const auto& tabletInfo1 = response1->Record.GetTablets(0);
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo1.GetTabletID(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL((int)tabletInfo1.GetVolatileState(), (int)NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_RUNNING);

        // killing hive
        runtime.Register(CreateTabletKiller(hiveTablet));

        // waiting for node synchronization
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvStatus));
            runtime.DispatchEvents(options);
        }

        // second check
        MakeSureTabletIsUp(runtime, tabletId, nodeIndex);

        runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo(tabletId));
        TAutoPtr<IEventHandle> handle2;
        TEvHive::TEvResponseHiveInfo* response2 = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle2);
        UNIT_ASSERT_VALUES_EQUAL(response2->Record.TabletsSize(), 1);
        const auto& tabletInfo2 = response2->Record.GetTablets(0);
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo2.GetTabletID(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL((int)tabletInfo2.GetVolatileState(), (int)NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_RUNNING);

        // the most important check
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo2.GetGeneration(), tabletInfo1.GetGeneration());
    }

    Y_UNIT_TEST(TestLimitedNodeList) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // Kill local on node 1
        SendKillLocal(runtime, 1);
        // Create the tablet
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.AddAllowedNodeIDs(runtime.GetNodeId(1));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, false);
        // Make sure the tablet is down
        MakeSureTabletIsDown(runtime, tabletId, 0);
        // Re-create the local on node 1
        CreateLocal(runtime, 1);
        // Make sure the tablet is created OK on node 1
        MakeSureTabletIsUp(runtime, tabletId, 1);
    }

    Y_UNIT_TEST(TestCreateTabletAndReassignGroups) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);
        MakeSureTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestCreateTabletWithWrongSPoolsAndReassignGroupsFailButDeletionIsOk) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TChannelsBindings channlesBinds = {GetDefaultChannelBind("NoExistStoragePool"),
                                           GetDefaultChannelBind("NoExistStoragePool")};
        auto ev = new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, channlesBinds);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, THolder(ev), 0, false);

        MakeSureTabletIsDown(runtime, tabletId, 0);

        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);

        /*{
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerSelectGroupsResult));
            runtime.DispatchEvents(options);
        }*/

        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        MakeSureTheTabletIsDeleted(runtime, hiveTablet, tabletId);
    }

    Y_UNIT_TEST(TestCreateTabletAndReassignGroups3) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true, 3);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);
        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);
        SendReassignTablet(runtime, hiveTablet, tabletId, {}, 0);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        {
            TDispatchOptions options;
            //options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerSelectGroups));
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestCreateTabletAndMixedReassignGroups3) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true, 3);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        TActorId sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo({
            .TabletId = tabletId,
            .ReturnChannelHistory = true,
        }));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        std::unordered_set<ui32> tabletGroups;
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            for (const NKikimrHive::TTabletChannelInfo& channel : tablet.GetTabletChannels()) {
                for (const NKikimrHive::TTabletChannelGenInfo& history : channel.GetHistory()) {
                    tabletGroups.insert(history.GetGroup());
                }
            }
        }

        auto updateDiskStatus = MakeHolder<TEvBlobStorage::TEvControllerUpdateDiskStatus>();

        for (ui32 groupId = 0x80000000; groupId < 0x8000000a; ++groupId) {
            if (tabletGroups.count(groupId) == 0) {
                NKikimrBlobStorage::TVDiskMetrics* vdiskMetrics = updateDiskStatus->Record.AddVDisksMetrics();

                vdiskMetrics->MutableVDiskId()->SetGroupID(groupId);
                vdiskMetrics->MutableVDiskId()->SetGroupGeneration(1);
                vdiskMetrics->MutableVDiskId()->SetRing(0);
                vdiskMetrics->MutableVDiskId()->SetDomain(0);
                vdiskMetrics->MutableVDiskId()->SetVDisk(0);
                vdiskMetrics->SetAvailableSize(100000);
            }
        }

        runtime.SendToPipe(MakeBSControllerID(0), sender, updateDiskStatus.Release(), 0, GetPipeConfigWithRetries());

        SendReassignTabletSpace(runtime, hiveTablet, tabletId, {}, 0);
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
            runtime.DispatchEvents(options);
        }
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TChannelsBindings newBindings = BINDED_CHANNELS;
        newBindings.push_back(GetChannelBind(STORAGE_POOL + "3")); // add one more channel

        // re-create tablet to apply new channel bindings
        SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, newBindings), 0, true);

        MakeSureTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestReassignGroupsWithRecreateTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true, 3);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime,
                                             hiveTablet,
                                             testerTablet,
                                             MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS),
                                             0,
                                             true);

        MakeSureTabletIsUp(runtime, tabletId, 0);

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvLookupChannelInfo(tabletId));
        TAutoPtr<IEventHandle> handle1;
        TEvHive::TEvChannelInfo* channelInfo1 = runtime.GrabEdgeEventRethrow<TEvHive::TEvChannelInfo>(handle1);
        TVector<ui32> channels = {1, 2};

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvInvalidateStoragePools(), 0, GetPipeConfigWithRetries());

        SendReassignTablet(runtime, hiveTablet, tabletId, channels, 0);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerSelectGroups));
            runtime.DispatchEvents(options);
        }

        tabletId = SendCreateTestTablet(runtime,
                                        hiveTablet,
                                        testerTablet,
                                        MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS),
                                        0,
                                        true,
                                        NKikimrProto::OK);

        MakeSureTabletIsUp(runtime, tabletId, 0);

        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvLookupChannelInfo(tabletId));
        TAutoPtr<IEventHandle> handle2;
        TEvHive::TEvChannelInfo* channelInfo2 = runtime.GrabEdgeEventRethrow<TEvHive::TEvChannelInfo>(handle2);
        UNIT_ASSERT_VALUES_EQUAL(channelInfo1->Record.ChannelInfoSize(), channelInfo2->Record.ChannelInfoSize());
        int size = channelInfo1->Record.ChannelInfoSize();
        for (int channel = 0; channel < size; ++channel) {
            if (std::find(channels.begin(), channels.end(), channel) != channels.end())
                continue;
            const auto& history1 = channelInfo1->Record.GetChannelInfo(channel).GetHistory();
            const auto& history2 = channelInfo2->Record.GetChannelInfo(channel).GetHistory();
            UNIT_ASSERT_VALUES_EQUAL_C(history1.size(), history2.size(), "For channel " << channel);
        }
    }

    Y_UNIT_TEST(TestCreateTabletAndReassignGroupsWithReboots) {
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 bsControllerTablet = MakeBSControllerID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);

        THiveInitialEventsFilter initialEventsFilter;

        RunTestWithReboots({hiveTablet, bsControllerTablet, testerTablet}, [&]() {
            return initialEventsFilter.Prepare();
        }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
            if (ENABLE_DETAILED_HIVE_LOG) {
                Ctest << "At dispatch " << dispatchName << Endl;
            }
            TTestBasicRuntime runtime;
            Setup(runtime, true);
            setup(runtime);
            TActorId sender = runtime.AllocateEdgeActor();

            CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
            runtime.DispatchEvents(options);

            ui64 tabletId = 0;
            TTabletTypes::EType tabletType = TTabletTypes::Dummy;

            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
            TAutoPtr<IEventHandle> handle;
            auto createTabletReply = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(handle);
            UNIT_ASSERT(createTabletReply);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetStatus(), NKikimrProto::OK,
                (ui32)createTabletReply->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
            UNIT_ASSERT_EQUAL_C(createTabletReply->Record.GetOwner(), testerTablet,
                createTabletReply->Record.GetOwner() << " != " << testerTablet);
            tabletId = createTabletReply->Record.GetTabletID();

            runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvReassignTablet(tabletId), 0, GetPipeConfigWithRetries());

            activeZone = true;
            {
                bool allowIncompleteResult = (dispatchName != INITIAL_TEST_DISPATCH_NAME);
                try {
                    TAutoPtr<IEventHandle> handle;
                    auto tabletCreationResult = runtime.GrabEdgeEventRethrow<TEvHive::TEvTabletCreationResult>(handle);
                    UNIT_ASSERT(tabletCreationResult);
                    UNIT_ASSERT_EQUAL_C(tabletCreationResult->Record.GetStatus(), NKikimrProto::OK,
                        (ui32)tabletCreationResult->Record.GetStatus() << " != " << (ui32)NKikimrProto::OK);
                    UNIT_ASSERT_EQUAL_C(tabletCreationResult->Record.GetTabletID(), tabletId,
                        tabletCreationResult->Record.GetTabletID() << " != " << tabletId);
                } catch (TEmptyEventQueueException&) {
                    Ctest << "Event queue is empty at dispatch " << dispatchName << "\n";
                    if (!allowIncompleteResult)
                        throw;
                }
            }
            activeZone = false;
        });
    }

//    Y_UNIT_TEST(TestCreateTabletAndChangeProfiles) {
//        TTestBasicRuntime runtime(1, false);
//        Setup(runtime, true);
//        TActorId sender = runtime.AllocateEdgeActor();
//        CreatePDiskAndGroup(runtime, sender);
//        const ui64 hiveTablet = MakeDefaultHiveID(0);
//        const ui64 testerTablet = MakeDefaultHiveID(1);
//        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

//        ui32 tabletType = 0;
//        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
//            new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, 0), 0, true);
//        MakeSureTabletIsUp(runtime, tabletId, 0);

//        { // setup channel profiles
//            TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
//            channelProfiles->Profiles.emplace_back();
//            TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
//            for (ui32 channelIdx = 0; channelIdx < 4; ++channelIdx) {
//                profile.Channels.emplace_back(TBlobStorageGroupType::Erasure4Plus2Block, 0, NKikimrBlobStorage::TVDiskKind::Default);
//            }
//            runtime.GetAppData().ChannelProfiles = channelProfiles;
//        }

//        tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
//            new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, 0), 0, true);
//        MakeSureTabletIsUp(runtime, tabletId, 0);
//    }

    // FIXME: Hive does not pass this test.
    // Commented to remove noise from the unit-test logs
    /*
    Y_UNIT_TEST(topTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, 0), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        SendStopTablet(runtime, hiveTablet, tabletId, 0);
        MakeSureTabletIsDown(runtime, tabletId, 0);
    }
    */

    // FIXME: Hive does not pass this test.
    // Commented to remove noise from the unit-test logs
    /*
    Y_UNIT_TEST(TestStopAndRestartTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        TVector<ui64> tabletIds;
        TAutoPtr<ITabletScheduledEventsGuard> guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, 0), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        SendStopTablet(runtime, hiveTablet, tabletId, 0);
        MakeSureTabletIsDown(runtime, tabletId, 0);
        ui64 tabletId2 = SendCreateTestTablet(runtime, hiveTablet, testerTablet,
            new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, 0), 0, true, NKikimrProto::ALREADY);
        UNIT_ASSERT_C(tabletId2 == tabletId, tabletId2 << " != " << tabletId);
        MakeSureTabletIsUp(runtime, tabletId, 0);
    }
    */

    /*
    Y_UNIT_TEST(TestFailureNotification) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        TActorId senderA = runtime.AllocateEdgeActor();
        TAutoPtr<ITabletScheduledEventsGuard> guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, senderA);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // Kill local on node 1
        SendKillLocal(runtime, 1);
        // Create the tablet
        ui32 tabletType = 0;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, 0));
        TVector<ui32> allowedNodes;
        allowedNodes.push_back(runtime.GetNodeId(1));
        ev->SetStartupOptions(TEvHive::TEvCreateTablet::FlagLimitAllowedNodes, &allowedNodes, 0, 0);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, ev.Release(), 0);
        // Make sure the tablet is down
        MakeSureTabletIsDown(runtime, tabletId, 0);
        // Re-create the local on node 1
        CreateLocal(runtime, 1);
        // Make sure the tablet is created OK on node 1
        MakeSureTabletIsUp(runtime, tabletId, 1);
    }
    */

    Y_UNIT_TEST(TestFollowers) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(2);
        followerGroup->SetRequireDifferentNodes(true);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;

        MakeSureTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        MakeSureTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        MakeSureTabletIsUp(runtime, tabletId, 2, &pipeConfig);
    }

    Y_UNIT_TEST(TestFollowersReconfiguration) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        TActorId senderA = runtime.AllocateEdgeActor();
        //TAutoPtr<ITabletScheduledEventsGuard> guard = CreateTabletScheduledEventsGuard(tabletIds, runtime, senderA);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(2);
        followerGroup->SetRequireDifferentNodes(true);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 2, &pipeConfig);

        ev.Reset(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetFollowerCount(1);
        runtime.SendToPipe(hiveTablet, senderA, ev.Release(), 0, GetPipeConfigWithRetries());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletDead);
            runtime.DispatchEvents(options);
        }

        ev.Reset(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetFollowerCount(2);
        runtime.SendToPipe(hiveTablet, senderA, ev.Release(), 0, GetPipeConfigWithRetries());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletActive);
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(TestFollowerPromotion) {
        constexpr int NODES = 3;
        TTestBasicRuntime runtime(NODES, false);
        Setup(runtime, true);

        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(2);
        followerGroup->SetAllowLeaderPromotion(true);

        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;
        std::array<bool, NODES> tabletRolesBefore = {};
        for (int i = 0; i < NODES; ++i) {
            MakeSureTabletIsUp(runtime, tabletId, i, &pipeConfig, &tabletRolesBefore[i]);
        }
        int leaders = std::accumulate(tabletRolesBefore.begin(), tabletRolesBefore.end(), 0, [](int a, bool b) -> int { return b ? a + 1 : a; });
        UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
        int leaderNode = std::find(tabletRolesBefore.begin(), tabletRolesBefore.end(), true) - tabletRolesBefore.begin();
        // killing leader
        SendKillLocal(runtime, leaderNode);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvTabletStatus);
            runtime.DispatchEvents(options);
        }
        std::array<bool, NODES> tabletRolesIntermediate = {};
        for (int i = 0; i < NODES; ++i) {
            if (i != leaderNode) {
                MakeSureTabletIsUp(runtime, tabletId, i, &pipeConfig, &tabletRolesIntermediate[i]);
            } else {
                tabletRolesIntermediate[i] = false;
            }
        }
        leaders = std::accumulate(tabletRolesIntermediate.begin(), tabletRolesIntermediate.end(), 0, [](int a, bool b) -> int { return b ? a + 1 : a; });
        int followers = std::accumulate(tabletRolesIntermediate.begin(), tabletRolesIntermediate.end(), 0, [](int a, bool b) -> int { return b ? a : a + 1; });
        UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
        UNIT_ASSERT_VALUES_EQUAL(followers, 2);
        std::unordered_set<std::pair<TTabletId, TFollowerId>> activeTablets;
        TActorId senderA = runtime.AllocateEdgeActor();
        for (int i = 0; i < NODES; ++i) {
            TActorId whiteboard = NNodeWhiteboard::MakeNodeWhiteboardServiceId(runtime.GetNodeId(i));
            runtime.Send(new IEventHandle(whiteboard, senderA, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest()));
            TAutoPtr<IEventHandle> handle;
            NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse* response = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse>(handle);
            for (const NKikimrWhiteboard::TTabletStateInfo& tabletInfo : response->Record.GetTabletStateInfo()) {
                if (tabletInfo.GetTabletId() == tabletId && (
                        tabletInfo.GetState() == NKikimrWhiteboard::TTabletStateInfo::Active ||
                        tabletInfo.GetState() == NKikimrWhiteboard::TTabletStateInfo::ResolveLeader))
                {
                    activeTablets.insert({tabletInfo.GetTabletId(), tabletInfo.GetFollowerId()});
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(activeTablets.size(), 3);
    }

    Y_UNIT_TEST(TestManyFollowersOnOneNode) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TVector<ui64> tabletIds;
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(3);
        followerGroup->SetAllowLeaderPromotion(true);
        SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        /*{
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvTabletStatus, 4);
            runtime.DispatchEvents(options);
        }*/
        // checking distribution, should be equal number of tablets on every node
        {
            std::array<int, 2> nodeTablets = {};
            {
                runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo(true));
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < 2),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(nodeTablets[0], 2);
            UNIT_ASSERT_VALUES_EQUAL(nodeTablets[1], 2);
        }
    }

    Y_UNIT_TEST(TestStartTabletTwiceInARow) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, MakeHolder<TEvHive::TEvCreateTablet>(testerTablet, 0, tabletType, BINDED_CHANNELS), 0, false);
        SendKillLocal(runtime, 0);
        CreateLocal(runtime, 0);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvBootTablet);
            runtime.DispatchEvents(options);
        }
        SendKillLocal(runtime, 0);
        CreateLocal(runtime, 0);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvBootTablet);
            runtime.DispatchEvents(options);
        }
        Y_UNUSED(tabletId);
    }

    Y_UNIT_TEST(TestHiveBalancer) {
        static const int NUM_NODES = 3;
        static const int NUM_TABLETS = NUM_NODES * 3;
        TTestBasicRuntime runtime(NUM_NODES, false);
        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }
        for (int nodeIdx = 0; nodeIdx < NUM_NODES; ++nodeIdx) {
            TActorId senderLocal = runtime.AllocateEdgeActor(nodeIdx);
            THolder<TEvHive::TEvTabletMetrics> ev = MakeHolder<TEvHive::TEvTabletMetrics>();
            ev->Record.MutableTotalResourceUsage()->SetCPU(999); // KIKIMR-9870
            runtime.SendToPipe(hiveTablet, senderLocal, ev.Release(), nodeIdx, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            TEvLocal::TEvTabletMetricsAck* response = runtime.GrabEdgeEvent<TEvLocal::TEvTabletMetricsAck>(handle);
            Y_UNUSED(response);
        }

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetObjectId(i);
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, should be equal number of tablets on every node
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                    Ctest << "tablet " << tablet.GetTabletID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.first, nodeTablets.begin());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.second, nodeTablets.end() - 1);
        }

        THashMap<ui64, ui64> tabletMetrics;

        // reporting uneven metrics for tablets
        {
            int i = 1;
            for (ui64 tabletId : tablets) {
                THolder<TEvHive::TEvTabletMetrics> metrics = MakeHolder<TEvHive::TEvTabletMetrics>();
                NKikimrHive::TTabletMetrics* metric = metrics->Record.AddTabletMetrics();
                metric->SetTabletID(tabletId);
                metric->MutableResourceUsage()->SetNetwork(100000 * i);
                tabletMetrics[tabletId] = 100000 * i;
                i *= 2;
                runtime.SendToPipe(hiveTablet, senderA, metrics.Release());
                TAutoPtr<IEventHandle> handle;
                runtime.GrabEdgeEventRethrow<TEvLocal::TEvTabletMetricsAck>(handle);
            }
        }

        // killing all tablets
        for (ui64 tabletId : tablets) {
            runtime.Register(CreateTabletKiller(tabletId));

            // wait for tablet to stop and start back up again
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvTabletStatus, 2);
            runtime.DispatchEvents(options);
        }

        // checking distribution, should be almost all tablets on one node and two other tablets on two other nodes (7,1,1)
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    ui32 nodeId = tablet.GetNodeID() - nodeBase;
                    nodeTablets[nodeId]++;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(1, *mmElements.first);
            UNIT_ASSERT_VALUES_EQUAL(7, *mmElements.second);
        }

        // creating NUM_TABLETS more tablets (with empty metrics)
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 200500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetObjectId(NUM_TABLETS + i);
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
        }

        // checking distribution, new tablets should go to less loaded nodes (7,6,5)
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                TEvHive::TEvRequestHiveInfo* request = new TEvHive::TEvRequestHiveInfo();
                request->Record.SetReturnMetrics(true);
                runtime.SendToPipe(hiveTablet, senderA, request);
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    ui32 nodeId = tablet.GetNodeID() - nodeBase;
                    nodeTablets[nodeId]++;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(2, *mmElements.first);
            UNIT_ASSERT_VALUES_EQUAL(11, *mmElements.second);
        }
    }

    TNodeLocation GetLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter(ToString(nodeId / 2 + 1));
        location.SetModule("1");
        location.SetRack("1");
        location.SetUnit("1");
        return TNodeLocation(location); // DC = [1,1,2,2,3,3]
    }

    Y_UNIT_TEST(TestHiveBalancerWithPrefferedDC1) {
        static const int NUM_NODES = 6;
        static const int NUM_TABLETS = NUM_NODES * 3;
        TTestBasicRuntime runtime(NUM_NODES, false);

        runtime.LocationCallback = GetLocation;

        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetFollowerCount(3);
            ev->Record.MutableDataCentersPreference()->AddDataCentersGroups()->AddDataCenter(ToString(1));
            ev->Record.MutableDataCentersPreference()->AddDataCentersGroups()->AddDataCenter(ToString(2));
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, all leaders should be on the first node
        {
            std::array<int, NUM_NODES> nodeLeaders = {};
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    if (tablet.GetFollowerID() == 0) {
                        nodeLeaders[tablet.GetNodeID() - nodeBase]++;
                    }
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            UNIT_ASSERT_GT(nodeLeaders[0], 0);
            UNIT_ASSERT_GT(nodeLeaders[1], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[2], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[3], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[4], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[5], 0);
            UNIT_ASSERT_GT(nodeTablets[0], 0);
            UNIT_ASSERT_GT(nodeTablets[1], 0);
            UNIT_ASSERT_GT(nodeTablets[2], 0);
            UNIT_ASSERT_GT(nodeTablets[3], 0);
            UNIT_ASSERT_GT(nodeTablets[4], 0);
            UNIT_ASSERT_GT(nodeTablets[5], 0);
        }
    }

    Y_UNIT_TEST(TestHiveBalancerWithPrefferedDC2) {
        static const int NUM_NODES = 6;
        static const int NUM_TABLETS = NUM_NODES * 3;
        TTestBasicRuntime runtime(NUM_NODES, false);

        runtime.LocationCallback = GetLocation;

        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetFollowerCount(3);
            auto* group = ev->Record.MutableDataCentersPreference()->AddDataCentersGroups();
            group->AddDataCenter(ToString(1));
            group->AddDataCenter(ToString(2));
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, all leaders should be on the first node
        {
            std::array<int, NUM_NODES> nodeLeaders = {};
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    if (tablet.GetFollowerID() == 0) {
                        nodeLeaders[tablet.GetNodeID() - nodeBase]++;
                    }
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            UNIT_ASSERT_GT(nodeLeaders[0], 0);
            UNIT_ASSERT_GT(nodeLeaders[1], 0);
            UNIT_ASSERT_GT(nodeLeaders[2], 0);
            UNIT_ASSERT_GT(nodeLeaders[3], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[4], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeLeaders[5], 0);
            UNIT_ASSERT_GT(nodeTablets[0], 0);
            UNIT_ASSERT_GT(nodeTablets[1], 0);
            UNIT_ASSERT_GT(nodeTablets[2], 0);
            UNIT_ASSERT_GT(nodeTablets[3], 0);
            UNIT_ASSERT_GT(nodeTablets[4], 0);
            UNIT_ASSERT_GT(nodeTablets[5], 0);
        }
    }

    Y_UNIT_TEST(TestHiveFollowersWithChangingDC) {
        static const int NUM_NODES = 6;
        static const int NUM_TABLETS = 1;
        TTestBasicRuntime runtime(NUM_NODES, false);

        runtime.LocationCallback = GetLocation;

        Setup(runtime, false);
        //const int nodeBase = runtime.GetNodeId(0);
        CreateLocal(runtime, 0);
        CreateLocal(runtime, 1);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetCrossDataCenterFollowerCount(1);
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, all leaders should be on the first node
        {
            int leaders = 0;
            int tablets = 0;
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    if (tablet.GetFollowerID() == 0) {
                        leaders++;
                    }
                    tablets++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(tablets, 2);
        }

        CreateLocal(runtime, 2);
        CreateLocal(runtime, 3);

        // kill all tablets
        for (ui64 tabletId : tablets) {
            runtime.Register(CreateTabletKiller(tabletId));

            // wait for tablet to stop and start back up again
            TDispatchOptions options;
             // leader (death, start) + new extra follower
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvTabletStatus, 3));
            runtime.DispatchEvents(options);
        }

        {
            int leaders = 0;
            int tablets = 0;
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    if (tablet.GetFollowerID() == 0) {
                        leaders++;
                    }
                    tablets++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(tablets, 3);
        }

        CreateLocal(runtime, 4);
        CreateLocal(runtime, 5);

        // kill all tablets
        for (ui64 tabletId : tablets) {
            runtime.Register(CreateTabletKiller(tabletId));

            // wait for tablet to stop and start back up again
            TDispatchOptions options;
             // leader (death, start) + new extra follower
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvLocal::EvTabletStatus, 3));
            runtime.DispatchEvents(options);
        }

        {
            int leaders = 0;
            int tablets = 0;
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    if (tablet.GetFollowerID() == 0) {
                        leaders++;
                    }
                    tablets++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(tablets, 4);
        }

        SendKillLocal(runtime, 2);
        SendKillLocal(runtime, 3);
        SendKillLocal(runtime, 4);
        SendKillLocal(runtime, 5);

        {
            int leaders = 0;
            int tablets = 0;
            int iterations = 100;
            while (--iterations > 0) {
                leaders = 0;
                tablets = 0;
                {
                    THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                    request->Record.SetReturnFollowers(true);
                    runtime.SendToPipe(hiveTablet, senderA, request.Release());
                    TAutoPtr<IEventHandle> handle;
                    TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                    for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                        if (tablet.GetFollowerID() == 0) {
                            leaders++;
                        }
                        tablets++;
                        Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID() << Endl;
                    }
                }
                if (leaders == 1 && tablets == 2) {
                    break;
                }
                runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(tablets, 2);
        }
    }

    Y_UNIT_TEST(TestHiveBalancerWithSystemTablets) {
        static const int NUM_NODES = 6;
        static const int NUM_TABLETS = 12;
        TTestBasicRuntime runtime(NUM_NODES, false);

        runtime.LocationCallback = GetLocation;

        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Mediator;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, all leaders should be on the first node
        {
            std::unordered_map<TString, ui64> dcTablets;
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    dcTablets[runtime.LocationCallback(tablet.GetNodeID() - nodeBase).GetDataCenterId()]++;
                    Ctest << "tablet " << tablet.GetTabletID() << "." << tablet.GetFollowerID() << " on node " << tablet.GetNodeID()
                          << " on DC " << runtime.LocationCallback(tablet.GetNodeID() - nodeBase).GetDataCenterId() << Endl;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(dcTablets.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(dcTablets.begin()->second, NUM_TABLETS);
        }
    }

    Y_UNIT_TEST(TestHiveBalancerWithFollowers) {
        static const int NUM_NODES = 8;
        static const int NUM_TABLETS = 24;
        TTestBasicRuntime runtime(NUM_NODES, false);
        Setup(runtime, true);
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        MakeSureTabletIsUp(runtime, hiveTablet, 0);

        // create NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetObjectId(1);
            ev->Record.SetFollowerCount(3);
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // check leader distribution, should be equal number of tablets on every node
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    Ctest << tablet.ShortDebugString() << Endl;
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.first, nodeTablets.begin());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.second, nodeTablets.end() - 1);
        }

        // check total distribution, should be equal number of tablets on every node
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
                request->Record.SetReturnFollowers(true);
                runtime.SendToPipe(hiveTablet, senderA, request.Release());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    Ctest << tablet.ShortDebugString() << Endl;
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.first, nodeTablets.begin());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.second, nodeTablets.end() - 1);
        }

        THashMap<ui64, ui64> tabletMetrics;

        // report metrics for leaders only
        {
            for (ui64 tabletId : tablets) {
                THolder<TEvHive::TEvTabletMetrics> metrics = MakeHolder<TEvHive::TEvTabletMetrics>();
                NKikimrHive::TTabletMetrics* metric = metrics->Record.AddTabletMetrics();
                metric->SetTabletID(tabletId);
                metric->MutableResourceUsage()->SetCPU(5000);
                runtime.SendToPipe(hiveTablet, senderA, metrics.Release());
                TAutoPtr<IEventHandle> handle;
                runtime.GrabEdgeEventRethrow<TEvLocal::TEvTabletMetricsAck>(handle);
            }
        }

        // kill all tablets
        for (ui64 tabletId : tablets) {
            runtime.Register(CreateTabletKiller(tabletId));

            // wait for tablet to stop and start back up again
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvTabletStatus, 2);
            runtime.DispatchEvents(options);
        }

        // check distribution, should be equal number of tablets on every node
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                }
            }
            auto mmElements = std::minmax_element(nodeTablets.begin(), nodeTablets.end());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.first, nodeTablets.begin());
            UNIT_ASSERT_VALUES_EQUAL(mmElements.second, nodeTablets.end() - 1);
        }
    }

    Y_UNIT_TEST(TestHiveBalancerWithLimit) {
        static const int NUM_NODES = 3;
        static const int NUM_TABLETS = NUM_NODES * 3;
        TTestBasicRuntime runtime(NUM_NODES, false);
        Setup(runtime, true);
        SendKillLocal(runtime, 0);
        SendKillLocal(runtime, 1);
        TLocalConfig::TPtr local0 = new TLocalConfig();
        {
            local0->TabletClassInfo[TTabletTypes::Dummy].SetupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet,
                TMailboxType::Simple, 0,
                TMailboxType::Simple, 0);
            local0->TabletClassInfo[TTabletTypes::Dummy].MaxCount = 2;
        }
        CreateLocal(runtime, 0, local0); // max 2 dummies on 0
        TLocalConfig::TPtr local1 = new TLocalConfig();
        {
            // it can't be empty, otherwise it will fallback to default behavior
            local1->TabletClassInfo[TTabletTypes::Unknown].SetupInfo = nullptr;
        }
        CreateLocal(runtime, 1, local1); // no tablets on 1
        const int nodeBase = runtime.GetNodeId(0);
        TActorId senderA = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvStatus, NUM_NODES);
            runtime.DispatchEvents(options);
        }
        for (int nodeIdx = 0; nodeIdx < NUM_NODES; ++nodeIdx) {
            TActorId senderLocal = runtime.AllocateEdgeActor(nodeIdx);
            THolder<TEvHive::TEvTabletMetrics> ev = MakeHolder<TEvHive::TEvTabletMetrics>();
            ev->Record.MutableTotalResourceUsage()->SetCPU(999); // KIKIMR-9870
            runtime.SendToPipe(hiveTablet, senderLocal, ev.Release(), nodeIdx, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            TEvLocal::TEvTabletMetricsAck* response = runtime.GrabEdgeEvent<TEvLocal::TEvTabletMetricsAck>(handle);
            Y_UNUSED(response);
        }

        // creating NUM_TABLETS tablets
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        TVector<ui64> tablets;
        for (int i = 0; i < NUM_TABLETS; ++i) {
            THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500 + i, tabletType, BINDED_CHANNELS));
            ev->Record.SetObjectId(i);
            ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
            tablets.emplace_back(tabletId);
            MakeSureTabletIsUp(runtime, tabletId, 0);
        }

        // checking distribution, should be equal number of tablets on every node
        {
            std::array<int, NUM_NODES> nodeTablets = {};
            {
                runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo());
                TAutoPtr<IEventHandle> handle;
                TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
                for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
                    UNIT_ASSERT_C(((int)tablet.GetNodeID() - nodeBase >= 0) && (tablet.GetNodeID() - nodeBase < NUM_NODES),
                            "nodeId# " << tablet.GetNodeID() << " nodeBase# " << nodeBase);
                    nodeTablets[tablet.GetNodeID() - nodeBase]++;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(nodeTablets[0], 2);
            UNIT_ASSERT_VALUES_EQUAL(nodeTablets[1], 0);
            UNIT_ASSERT_VALUES_EQUAL(nodeTablets[2], NUM_TABLETS - 2);
        }
    }

    Y_UNIT_TEST(TestRestartTablets) {
        TTestBasicRuntime runtime(3, false);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetAllowFollowerPromotion(false);
        ev->Record.SetFollowerCount(2);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.ForceLocal = true;
        pipeConfig.AllowFollower = true;

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 2, &pipeConfig);

        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(0)));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvDeadTabletAck);
            runtime.DispatchEvents(options);
        }

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 2, &pipeConfig);

        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(1)));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvDeadTabletAck);
            runtime.DispatchEvents(options);
        }

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 2, &pipeConfig);

        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(2)));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvDeadTabletAck);
            runtime.DispatchEvents(options);
        }

        WaitForTabletIsUp(runtime, tabletId, 0, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 1, &pipeConfig);
        WaitForTabletIsUp(runtime, tabletId, 2, &pipeConfig);
    }

    Y_UNIT_TEST(TestFollowersCrossDC_Easy) {
        TTestBasicRuntime runtime((ui32)9, (ui32)3);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetCrossDataCenterFollowerCount(2);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.ForceLocal = true;
        ui32 tabsPerDC[3] = {};
        ui32 leaders = 0;
        ui32 followers = 0;
        for (ui32 node = 0; node < 9; ++node) {
            bool leader;
            if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                if (leader) {
                    leaders++;
                    tabsPerDC[node % 3]++;
                }
            }
        }
        pipeConfig.AllowFollower = true;
        pipeConfig.ForceFollower = true;
        for (ui32 node = 0; node < 9; ++node) {
            bool leader;
            if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                if (!leader) {
                    followers++;
                    tabsPerDC[node % 3]++;
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
        UNIT_ASSERT_VALUES_EQUAL(followers, 6);
        UNIT_ASSERT(tabsPerDC[0] >= 2);
        UNIT_ASSERT(tabsPerDC[1] >= 2);
        UNIT_ASSERT(tabsPerDC[2] >= 2);
    }

    Y_UNIT_TEST(TestFollowers_LocalNodeOnly) {
        TTestBasicRuntime runtime((ui32)9, (ui32)3);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(1);
        followerGroup->SetLocalNodeOnly(true);
        followerGroup->SetAllowClientRead(true);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        ui32 leaderNode = 999;
        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.ForceLocal = true;

            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < 9; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        leaderNode = node;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            ui32 followerNode = 999;
            for (ui32 node = 0; node < 9; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        followers++;
                        followerNode = node;
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(followers, 1);
            UNIT_ASSERT_VALUES_EQUAL(leaderNode, followerNode);
        }

        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(leaderNode)));
        SendKillLocal(runtime, leaderNode);
        WaitForTabletsBecomeActive(runtime, 2);

        ui32 secondLeaderNode = 999;

        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.ForceLocal = true;

            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < 9; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        secondLeaderNode = node;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            ui32 followerNode = 999;
            for (ui32 node = 0; node < 9; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        followers++;
                        followerNode = node;
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(followers, 1);
            UNIT_ASSERT(leaderNode != secondLeaderNode);
            UNIT_ASSERT_VALUES_EQUAL(secondLeaderNode, followerNode);
        }
    }

    Y_UNIT_TEST(TestFollowersCrossDC_Tight) {
        static constexpr ui32 NODES = 9;
        static constexpr ui32 DCS = 3;
        static constexpr ui32 FOLLOWERS = NODES / DCS;
        TTestBasicRuntime runtime(NODES, DCS);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetCrossDataCenterFollowerCount(FOLLOWERS);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.ForceLocal = true;
        ui32 followersPerDC[DCS] = {};
        ui32 leaders = 0;
        ui32 followers = 0;
        for (ui32 node = 0; node < NODES; ++node) {
            bool leader;
            if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                if (leader) {
                    leaders++;
                }
            }
        }
        pipeConfig.AllowFollower = true;
        pipeConfig.ForceFollower = true;
        for (ui32 node = 0; node < NODES; ++node) {
            bool leader;
            if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                if (!leader) {
                    followers++;
                    followersPerDC[node % DCS]++;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
        UNIT_ASSERT_VALUES_EQUAL(followers, FOLLOWERS * DCS);
        for (ui32 dc = 0; dc < DCS; ++dc) {
            UNIT_ASSERT(followersPerDC[dc] == FOLLOWERS);
        }
    }

    Y_UNIT_TEST(TestFollowersCrossDC_MovingLeader) {
        static constexpr ui32 NODES = 9;
        static constexpr ui32 DCS = 3;
        static constexpr ui32 FOLLOWERS = NODES / DCS;
        TTestBasicRuntime runtime(NODES, DCS);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetCrossDataCenterFollowerCount(FOLLOWERS);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        //WaitForTabletsBecomeActive(runtime, 3 * 3 + 1);

        ui32 leadersNode = 0;
        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.ForceLocal = true;
            ui32 followersPerDC[DCS] = {};
            ui32 total = 0;
            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        leadersNode = node;
                        total++;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        total++;
                        followers++;
                        followersPerDC[node % DCS]++;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(total, 1 + FOLLOWERS * DCS);
            UNIT_ASSERT_VALUES_EQUAL(followers, FOLLOWERS * DCS);
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            for (ui32 dc = 0; dc < DCS; ++dc) {
                UNIT_ASSERT(followersPerDC[dc] == FOLLOWERS);
            }
        }

        runtime.Register(CreateTabletKiller(tabletId, runtime.GetNodeId(leadersNode)));
        WaitForTabletsBecomeActive(runtime, 1);

        {
            NTabletPipe::TClientConfig pipeConfig;
            // we need retry policy to handle possible follower reconnect
            pipeConfig.RetryPolicy = {.RetryLimitCount = 2, .MinRetryTime = TDuration::MilliSeconds(100)};
            pipeConfig.ForceLocal = true;
            ui32 followersPerDC[DCS] = {};
            ui32 total = 0;
            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        total++;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        total++;
                        followers++;
                        followersPerDC[node % DCS]++;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(total, 1 + FOLLOWERS * DCS);
            UNIT_ASSERT_VALUES_EQUAL(followers, FOLLOWERS * DCS);
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            for (ui32 dc = 0; dc < DCS; ++dc) {
                UNIT_ASSERT(followersPerDC[dc] == FOLLOWERS);
            }
        }
    }

    Y_UNIT_TEST(TestFollowersCrossDC_KillingHiveAndFollower) {
        static constexpr ui32 NODES = 3;
        static constexpr ui32 DCS = 3;
        static constexpr ui32 FOLLOWERS = 1;
        TTestBasicRuntime runtime(NODES, DCS);
        Setup(runtime, true);
        TVector<ui64> tabletIds;
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive, 0);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, runtime.GetNodeCount());
            runtime.DispatchEvents(options);
        }
        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 100500, tabletType, BINDED_CHANNELS));
        ev->Record.SetObjectId(1337);
        auto* followerGroup = ev->Record.AddFollowerGroups();
        followerGroup->SetFollowerCount(FOLLOWERS);
        followerGroup->SetFollowerCountPerDataCenter(true);
        followerGroup->SetRequireAllDataCenters(true);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        ui32 leaderNode = 0;
        ui32 followersNode = 0;
        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.ForceLocal = true;
            ui32 total = 0;
            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        total++;
                        leaderNode = node;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        total++;
                        followers++;
                        if (node != leaderNode) {
                            followersNode = node;
                        }
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(followers, FOLLOWERS * DCS);
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT_VALUES_EQUAL(total, 1 + FOLLOWERS * DCS);
        }

        runtime.Register(CreateTabletKiller(hiveTablet));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletDead);
            runtime.DispatchEvents(options);
        }
        SendKillLocal(runtime, followersNode);
        WaitForEvServerDisconnected(runtime);
        //WaitForTabletsBecomeActive(runtime, 1); // hive
        CreateLocal(runtime, followersNode);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvSyncTablets, NODES);
            runtime.DispatchEvents(options);
        }
        runtime.Register(CreateTabletKiller(tabletId));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvLocal::EvTabletStatus, 2);
            runtime.DispatchEvents(options);
        }

        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.ForceLocal = true;
            ui32 total = 0;
            ui32 leaders = 0;
            ui32 followers = 0;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (leader) {
                        leaders++;
                        total++;
                    }
                }
            }
            pipeConfig.AllowFollower = true;
            pipeConfig.ForceFollower = true;
            for (ui32 node = 0; node < NODES; ++node) {
                bool leader;
                if (CheckTabletIsUp(runtime, tabletId, node, &pipeConfig, &leader)) {
                    if (!leader) {
                        total++;
                        followers++;
                    }
                }
            }
            UNIT_ASSERT(followers >= (FOLLOWERS * DCS - 1));
            UNIT_ASSERT_VALUES_EQUAL(leaders, 1);
            UNIT_ASSERT(total >= FOLLOWERS * DCS);
        }
    }

    Y_UNIT_TEST(TestCreateExternalTablet) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ev->Record.SetTabletBootMode(NKikimrHive::TABLET_BOOT_MODE_EXTERNAL);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsDown(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestCreateTabletChangeToExternal) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);
        THolder<TEvHive::TEvCreateTablet> ev2(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ev2->Record.SetTabletBootMode(NKikimrHive::TABLET_BOOT_MODE_EXTERNAL);
        ui64 tabletId2 = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev2), 0, false, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(tabletId, tabletId2);
        MakeSureTabletIsDown(runtime, tabletId2, 0);
    }

    void SendGetTabletStorageInfo(TTestActorRuntime& runtime, ui64 hiveTablet, ui64 tabletId, ui32 nodeIndex) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvGetTabletStorageInfo(tabletId), nodeIndex, GetPipeConfigWithRetries());
    }

    Y_UNIT_TEST(TestGetStorageInfo) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, false);

        SendGetTabletStorageInfo(runtime, hiveTablet, tabletId, 0);

        TAutoPtr<IEventHandle> handle;
        auto getTabletStorageResult = runtime.GrabEdgeEventRethrow<TEvHive::TEvGetTabletStorageInfoResult>(handle);
        UNIT_ASSERT(getTabletStorageResult);
        UNIT_ASSERT_VALUES_EQUAL(getTabletStorageResult->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(getTabletStorageResult->Record.GetTabletID(), tabletId);
    }

    Y_UNIT_TEST(TestGetStorageInfoDeleteTabletBeforeAssigned) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        // Block group assignment
        runtime.SetObserverFunc([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::EvControllerSelectGroups) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(runtime, event);
        });

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, false);

        SendGetTabletStorageInfo(runtime, hiveTablet, tabletId, 0);

        // Must get a registered response
        {
            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEventRethrow<TEvHive::TEvGetTabletStorageInfoRegistered>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTabletID(), tabletId);
        }

        // Delete tablet while info request is pending
        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        // Must get a final response
        {
            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEventRethrow<TEvHive::TEvGetTabletStorageInfoResult>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTabletID(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetStatus(), NKikimrProto::ERROR);
        }
    }

    void SendLockTabletExecution(TTestActorRuntime& runtime, ui64 hiveTablet, ui64 tabletId, ui32 nodeIndex,
                                 NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK,
                                 const TActorId& owner = TActorId(), ui64 maxTimeout = 0,
                                 bool reconnect = false)
    {
        THolder<TEvHive::TEvLockTabletExecution> event(new TEvHive::TEvLockTabletExecution(tabletId));
        if (owner) {
            ActorIdToProto(owner, event->Record.MutableOwnerActor());
        }
        if (maxTimeout > 0) {
            event->Record.SetMaxReconnectTimeout(maxTimeout);
        }
        if (reconnect) {
            event->Record.SetReconnect(true);
        }
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, event.Release(), nodeIndex, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto result = runtime.GrabEdgeEventRethrow<TEvHive::TEvLockTabletExecutionResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTabletID(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), expectedStatus);
    }

    void VerifyLockTabletExecutionLost(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& owner) {
        TAutoPtr<IEventHandle> handle;
        auto result = runtime.GrabEdgeEventRethrow<TEvHive::TEvLockTabletExecutionLost>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(handle->GetRecipientRewrite(), owner);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTabletID(), tabletId);
    }

    Y_UNIT_TEST(TestLockTabletExecution) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        TActorId disconnecter = runtime.AllocateEdgeActor(0);
        TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        runtime.Send(new IEventHandle(proxy, disconnecter, new TEvInterconnect::TEvDisconnect()), 0);

        // Tablet should boot when the locking node disconnects
        WaitForTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestLockTabletExecutionBadOwner) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        // Owner cannot be on a different node
        TActorId owner = runtime.AllocateEdgeActor(0);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::ERROR, owner);
        MakeSureTabletIsUp(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestLockTabletExecutionTimeout) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 1000);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        TActorId disconnecter = runtime.AllocateEdgeActor(0);
        TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        runtime.Send(new IEventHandle(proxy, disconnecter, new TEvInterconnect::TEvDisconnect()), 0);

        // Tablet should boot when timeout expires
        WaitForTabletIsUp(runtime, tabletId, 0);

        // Hive should try to notify owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    Y_UNIT_TEST(TestLockTabletExecutionRebootTimeout) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        // Lock with a 40 second timeout (test reboots take 30 seconds)
        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 40000);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Reboot the hive tablet
        RebootTablet(runtime, hiveTablet, runtime.AllocateEdgeActor(0));

        // Tablet should boot when timeout expires
        WaitForTabletIsUp(runtime, tabletId, 0);

        // Hive should try to notify owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    Y_UNIT_TEST(TestLockTabletExecutionDelete) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        TActorId sender = runtime.AllocateEdgeActor();
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 1000);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Delete tablet while it is locked
        if (!SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0))) {
            WaitEvDeleteTabletResult(runtime);
        }

        // Make sure tablet does not exist anymore
        runtime.SendToPipe(hiveTablet, sender, new TEvHive::TEvRequestHiveInfo(true));
        TAutoPtr<IEventHandle> handle;
        TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        for (const NKikimrHive::TTabletInfo& tablet : response->Record.GetTablets()) {
            UNIT_ASSERT_VALUES_UNEQUAL(tablet.GetTabletID(), tabletId);
        }

        // Hive should try to notify owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    Y_UNIT_TEST(TestLockTabletExecutionDeleteReboot) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 1000);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Setup observer that would drop EvDeleteTabletResult messages
        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletBase::EvDeleteTabletResult) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return prevObserverFunc(runtime, event);
        });

        // Delete tablet while it is locked
        SendDeleteTestTablet(runtime, hiveTablet, MakeHolder<TEvHive::TEvDeleteTablet>(testerTablet, 0, 0));

        // Reboot hive while tablet deletion is still delayed.
        RebootTablet(runtime, hiveTablet, runtime.AllocateEdgeActor(0));

        // Remove observer and reboot hive one more time, letting delete to finish normally.
        runtime.SetObserverFunc(prevObserverFunc);
        RebootTablet(runtime, hiveTablet, runtime.AllocateEdgeActor(0));

        // Hive should try to notify owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    void MakeSureTabletStaysDown(TTestActorRuntime& runtime, ui64 tabletId, const TDuration& timeout) {
        TActorId edge = runtime.AllocateEdgeActor();
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.AllowFollower = true;
        clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        TActorId pipeClient = runtime.Register(NTabletPipe::CreateClient(edge, tabletId, clientConfig));
        TAutoPtr<IEventHandle> handle;
        TInstant deadline = TInstant::Now() + timeout;
        bool res = false;

        do {
            TEvTabletPipe::TEvClientConnected* ev = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(handle, timeout);
            if (!ev) {
                continue;
            }
            if (ev->TabletId == tabletId) {
                res = (ev->Status == NKikimrProto::OK);
                if (res) {
                    break;
                }
            }
        } while (TInstant::Now() <= deadline);

        runtime.Send(new IEventHandle(pipeClient, TActorId(), new TEvents::TEvPoisonPill()));
        UNIT_ASSERT_C(!res, "Unexpected successful tablet connection");
    }

    Y_UNIT_TEST(TestLockTabletExecutionReconnect) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        // lock with a 500ms timeout
        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 500);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // disconnect the node
        TActorId disconnecter = runtime.AllocateEdgeActor(0);
        TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        runtime.Send(new IEventHandle(proxy, disconnecter, new TEvInterconnect::TEvDisconnect()), 0);

        // reconnect the lock
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 500, true);
        MakeSureTabletStaysDown(runtime, tabletId, TDuration::MilliSeconds(1000));
    }

    Y_UNIT_TEST(TestLockTabletExecutionRebootReconnect) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        // Lock with a 40 second timeout (test reboots take 30 seconds)
        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 40000);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Reboot the hive tablet
        RebootTablet(runtime, hiveTablet, runtime.AllocateEdgeActor(0));

        // Reconnect the lock
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 40000, true);
    }

    Y_UNIT_TEST(TestLockTabletExecutionReconnectExpire) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        const TActorId hiveActor = CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);
        runtime.EnableScheduleForActor(hiveActor);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        // lock with a 500ms timeout
        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner, 500);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // disconnect the node
        TActorId disconnecter = runtime.AllocateEdgeActor(0);
        TActorId proxy = runtime.GetInterconnectProxy(0, 1);
        runtime.Send(new IEventHandle(proxy, disconnecter, new TEvInterconnect::TEvDisconnect()), 0);

        // wait for the lost lock notification
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);

        // lock reconnect should fail
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::ERROR, owner, 500, true);
    }

    void SendUnlockTabletExecution(TTestActorRuntime& runtime, ui64 hiveTablet, ui64 tabletId, ui32 nodeIndex,
                                   NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK,
                                   const TActorId& owner = TActorId())
    {
        THolder<TEvHive::TEvUnlockTabletExecution> event(new TEvHive::TEvUnlockTabletExecution(tabletId));
        if (owner) {
            ActorIdToProto(owner, event->Record.MutableOwnerActor());
        }
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, event.Release(), nodeIndex, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto result = runtime.GrabEdgeEventRethrow<TEvHive::TEvUnlockTabletExecutionResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTabletID(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), expectedStatus);
    }

    Y_UNIT_TEST(TestLockTabletExecutionBadUnlock) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Unlocking with a different owner (sender by default) is prohibited
        SendUnlockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::ERROR);
        MakeSureTabletIsDown(runtime, tabletId, 0);
    }

    Y_UNIT_TEST(TestLockTabletExecutionGoodUnlock) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Unlocking with the same owner should succeed and boot the tablet
        SendUnlockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner);
        WaitForTabletIsUp(runtime, tabletId, 0);

        // Hive should try to notify owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    Y_UNIT_TEST(TestLockTabletExecutionStealLock) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, false);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TTabletTypes::EType tabletType = TTabletTypes::Dummy;
        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, tabletType, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId owner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // Lock to a different owner
        TActorId owner2 = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, owner2);

        // Hive should notify the old owner on unlocking
        VerifyLockTabletExecutionLost(runtime, tabletId, owner);
    }

    Y_UNIT_TEST(TestExternalBoot) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, true);
        CreateLocal(runtime, 0); // only the 1st node has local running
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        TAutoPtr<TEvHive::TEvCreateTablet> ev = new TEvHive::TEvCreateTablet(testerTablet, 0, TTabletTypes::Dummy, BINDED_CHANNELS);
        ev->Record.SetTabletBootMode(NKikimrHive::ETabletBootMode::TABLET_BOOT_MODE_EXTERNAL);
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);

        TActorId owner1 = runtime.AllocateEdgeActor(0);
        runtime.SendToPipe(hiveTablet, owner1, new TEvHive::TEvInitiateTabletExternalBoot(tabletId), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto* result = runtime.GrabEdgeEvent<TEvLocal::TEvBootTablet>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetSuggestedGeneration(), 1);
        UNIT_ASSERT_EQUAL(result->Record.GetBootMode(), NKikimrLocal::EBootMode::BOOT_MODE_LEADER);

        const auto& storageInfo = result->Record.GetInfo();
        UNIT_ASSERT_EQUAL(storageInfo.GetTabletID(), tabletId);
        UNIT_ASSERT_EQUAL(storageInfo.GetTabletType(), TTabletTypes::Dummy);
        UNIT_ASSERT(storageInfo.ChannelsSize() > 0);
    }

    Y_UNIT_TEST(TestExternalBootWhenLocked) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, true);
        const ui64 hiveTablet = MakeDefaultHiveID(0);
        const ui64 testerTablet = MakeDefaultHiveID(1);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(hiveTablet, TTabletTypes::Hive), &CreateDefaultHive);

        THolder<TEvHive::TEvCreateTablet> ev(new TEvHive::TEvCreateTablet(testerTablet, 0, TTabletTypes::Dummy, BINDED_CHANNELS));
        ui64 tabletId = SendCreateTestTablet(runtime, hiveTablet, testerTablet, std::move(ev), 0, true);
        MakeSureTabletIsUp(runtime, tabletId, 0);

        TActorId bootOwner = runtime.AllocateEdgeActor(1);

        // cannot use external boot for normal tablets
        {
            runtime.SendToPipe(hiveTablet, bootOwner, new TEvHive::TEvInitiateTabletExternalBoot(tabletId), 1, GetPipeConfigWithRetries());

            auto result = runtime.GrabEdgeEvent<TEvHive::TEvBootTabletReply>(bootOwner);
            const auto* msg = result->Get();
            UNIT_ASSERT_EQUAL(msg->Record.GetStatus(), NKikimrProto::EReplyStatus::ERROR);
        }

        TActorId lockOwner = runtime.AllocateEdgeActor(1);
        SendLockTabletExecution(runtime, hiveTablet, tabletId, 1, NKikimrProto::OK, lockOwner);
        MakeSureTabletIsDown(runtime, tabletId, 0);

        // should be possible to boot it externally after locking
        {
            runtime.SendToPipe(hiveTablet, bootOwner, new TEvHive::TEvInitiateTabletExternalBoot(tabletId), 1, GetPipeConfigWithRetries());

            auto result = runtime.GrabEdgeEvent<TEvLocal::TEvBootTablet>(bootOwner);
            const auto* msg = result->Get();
            UNIT_ASSERT_EQUAL(msg->Record.GetBootMode(), NKikimrLocal::EBootMode::BOOT_MODE_LEADER);

            const auto& storageInfo = msg->Record.GetInfo();
            UNIT_ASSERT_EQUAL(storageInfo.GetTabletID(), tabletId);
            UNIT_ASSERT_EQUAL(storageInfo.GetTabletType(), TTabletTypes::Dummy);
            UNIT_ASSERT(storageInfo.ChannelsSize() > 0);
        }
    }
}

}
