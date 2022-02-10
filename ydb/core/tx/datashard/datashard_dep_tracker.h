#pragma once

#include "datashard.h"
#include "datashard_user_table.h"
#include "datashard_active_transaction.h"
#include "range_treap.h"

#include <library/cpp/containers/flat_hash/flat_hash.h>

namespace NKikimr {
namespace NDataShard {

/**
 * TDependencyTracker - tracks dependencies between operations
 */
class TDependencyTracker {
private:
    using TKeys = TVector<TOperationKey>;

    struct TOperationPtrTraits {
        static bool Less(const TOperation::TPtr& a, const TOperation::TPtr& b) noexcept {
            return a.Get() < b.Get();
        }

        static bool Equal(const TOperation::TPtr& a, const TOperation::TPtr& b) noexcept {
            return a.Get() == b.Get();
        }
    };

    /**
     * Predicts the state of a lock in the future
     */
    struct TLockPrediction {
        // For each table tracks the full list of ranges
        TKeys Keys;
        // When true the lock is known to be broken
        bool Broken = false;
        // When true the range is "everything in all tables"
        bool WholeShard = false;
        // When true lock has been imported from lock store
        bool Initialized = false;
    };

    /**
     * Tracks current per-table read/write sets
     */
    struct TTableState {
        // Mapping from read/write ranges to corresponding transactions
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> PlannedReads;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> PlannedWrites;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> ImmediateReads;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> ImmediateWrites;
    };

    struct TDependencyTrackingLogic { 
        TDependencyTracker& Parent; 
 
        explicit TDependencyTrackingLogic(TDependencyTracker& parent) 
            : Parent(parent) {} 
 
        // Adds operation to the tracker 
        virtual void AddOperation(const TOperation::TPtr& op) const noexcept = 0; 
 
        // Removes operation from the tracker, no future operations may conflict with it 
        virtual void RemoveOperation(const TOperation::TPtr& op) const noexcept = 0; 
    }; 
 
    struct TDefaultDependencyTrackingLogic : public TDependencyTrackingLogic { 
        explicit TDefaultDependencyTrackingLogic(TDependencyTracker& parent) 
            : TDependencyTrackingLogic(parent) {} 
 
        void AddOperation(const TOperation::TPtr& op) const noexcept override; 
        void RemoveOperation(const TOperation::TPtr& op) const noexcept override; 
    }; 
 
    struct TMvccDependencyTrackingLogic : public TDependencyTrackingLogic { 
        explicit TMvccDependencyTrackingLogic(TDependencyTracker& parent) 
            : TDependencyTrackingLogic(parent) {} 
 
        void AddOperation(const TOperation::TPtr& op) const noexcept override; 
        void RemoveOperation(const TOperation::TPtr& op) const noexcept override; 
    }; 
 
public:
    TDependencyTracker(TDataShard* self)
        : Self(self)
        , DefaultLogic(*this) 
        , MvccLogic(*this) 
    { }

public:
    // Called to update this table schema
    void UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo) noexcept;

    // Calld to update this table schema upon move
    void RemoveSchema(const TPathId& tableId) noexcept;

    // Adds operation to the tracker
    void AddOperation(const TOperation::TPtr& op) noexcept { 
        GetTrackingLogic().AddOperation(op); 
    } 

    // Removes operation from the tracker, no future operations may conflict with it
    void RemoveOperation(const TOperation::TPtr& op) noexcept { 
        GetTrackingLogic().RemoveOperation(op); 
    } 

private:
    void ClearTmpRead() noexcept;
    void ClearTmpWrite() noexcept;

    void AddPlannedReads(const TOperation::TPtr& op, const TKeys& reads) noexcept;
    void AddPlannedWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept;
    void AddImmediateReads(const TOperation::TPtr& op, const TKeys& reads) noexcept;
    void AddImmediateWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept;

    void FlushPlannedReads() noexcept;
    void FlushPlannedWrites() noexcept;
    void FlushImmediateReads() noexcept;
    void FlushImmediateWrites() noexcept;

    const TDependencyTrackingLogic& GetTrackingLogic() const noexcept; 
 
private:
    TDataShard* Self;
    // Temporary vectors for building dependencies
    TKeys TmpRead;
    TKeys TmpWrite;
    // Last planned snapshot operation
    TOperation::TPtr LastSnapshotOp;
    // Maps lock id to prediction of what this lock would look like when all transactions are complete
    NFH::TFlatHashMap<ui64, TLockPrediction> Locks;
    // Maps lock id to the last operation that used them, all lock operations are serialized
    NFH::TFlatHashMap<ui64, TOperation::TPtr> LastLockOps;
    // Maps table id to current ranges that have been read/written by all transactions
    NFH::TFlatHashMap<ui64, TTableState> Tables;
    // All operations that are reading or writing something
    TIntrusiveList<TOperationAllListItem> AllPlannedReaders;
    TIntrusiveList<TOperationAllListItem> AllPlannedWriters;
    TIntrusiveList<TOperationAllListItem> AllImmediateReaders;
    TIntrusiveList<TOperationAllListItem> AllImmediateWriters;
    // A set of operations that read/write table globally
    TIntrusiveList<TOperationGlobalListItem> GlobalPlannedReaders;
    TIntrusiveList<TOperationGlobalListItem> GlobalPlannedWriters;
    TIntrusiveList<TOperationGlobalListItem> GlobalImmediateReaders;
    TIntrusiveList<TOperationGlobalListItem> GlobalImmediateWriters;
    // Immediate operations that have delayed adding their keys to ranges
    TIntrusiveList<TOperationDelayedReadListItem> DelayedPlannedReads;
    TIntrusiveList<TOperationDelayedReadListItem> DelayedImmediateReads;
    TIntrusiveList<TOperationDelayedWriteListItem> DelayedPlannedWrites;
    TIntrusiveList<TOperationDelayedWriteListItem> DelayedImmediateWrites;
 
    const TDefaultDependencyTrackingLogic DefaultLogic; 
    const TMvccDependencyTrackingLogic MvccLogic; 
};

} // namespace NDataShard
} // namespace NKikimr
