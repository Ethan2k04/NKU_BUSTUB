#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    if (const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
        seq_scan.filter_predicate_ != nullptr) {
      if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan.filter_predicate_.get());
          cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
        const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
        const auto indices = catalog_.GetTableIndexes(table_info->name_);
        const auto *column_value_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->children_[0].get());

        for (const auto *index : indices) {
          const auto &columns = index->index_->GetKeyAttrs();
          std::vector<uint32_t> filter_column_ids = {column_value_expr->GetColIdx()};
          if (filter_column_ids == columns) {
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                       index->index_oid_, seq_scan.filter_predicate_);
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
