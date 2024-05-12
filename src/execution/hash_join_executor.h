//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** HashJoinKey represents a key in an hash join operation */
struct HashJoinKey {
  Value key_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent key_ expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on Value */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    return bustub::HashUtil::HashValue(&hash_key.key_);
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  auto LeftAntiJoinTuple(Tuple *left_tuple) -> Tuple;

  auto InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an HashJoinKey */
  auto MakeHashJoinLeftKey(const Tuple *tuple, const Schema &schema) -> std::vector<HashJoinKey> {
    std::vector<HashJoinKey> keys;
    auto left_expressions = plan_->LeftJoinKeyExpressions();
    keys.reserve(left_expressions.size());
    for (const auto &exp : left_expressions) {
      keys.push_back({exp->Evaluate(tuple, schema)});
    }
    return keys;
  }

  auto MakeHashJoinRightKey(const Tuple *tuple, const Schema &schema) -> std::vector<HashJoinKey> {
    std::vector<HashJoinKey> keys;
    auto right_expressions = plan_->RightJoinKeyExpressions();
    keys.reserve(right_expressions.size());
    for (const auto &exp : right_expressions) {
      keys.push_back({exp->Evaluate(tuple, schema)});
    }
    return keys;
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /* The hash table storing hash keys of left tuples */
  std::vector<std::unordered_map<HashJoinKey, std::vector<Tuple>>> hash_table_;
  /* The queue emitting result tuples */
  std::queue<Tuple> queue_;
  /* The map marking whether a left tuple has inner join result */
  std::unordered_map<HashJoinKey, bool> left_done_;
};

}  // namespace bustub