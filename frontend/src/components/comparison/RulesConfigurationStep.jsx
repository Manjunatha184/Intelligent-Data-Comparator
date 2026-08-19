import React from "react";
import KeyMapping from "./KeyMapping.jsx";
import GroupReconciliation from "./GroupReconciliation.jsx";
import ColumnMapping from "./ColumnMapping.jsx";
import FilterEditor from "./FilterEditor.jsx";
import IgnoredColumns from "./IgnoredColumns.jsx";
import RuleSelection from "./RuleSelection.jsx";

export default function RulesConfigurationStep(props) {
  const { levels = [] } = props;
  return <div className="stack rulesConfigurationStep">
    {(levels.includes("L2") || levels.includes("L3") || levels.includes("L4")) && <KeyMapping comparisonKeys={props.comparisonKeys} setComparisonKeys={props.setComparisonKeys} sourceSchema={props.sourceSchema} targetSchema={props.targetSchema} />}
    {levels.includes("L3") && <GroupReconciliation groupingAttributes={props.groupingAttributes} setGroupingAttributes={props.setGroupingAttributes} aggregationColumns={props.aggregationColumns} setAggregationColumns={props.setAggregationColumns} sourceSchema={props.sourceSchema} targetSchema={props.targetSchema} />}
    {levels.includes("L4") && <ColumnMapping mappings={props.columnMappings} setMappings={props.setColumnMappings} sourceSchema={props.sourceSchema} targetSchema={props.targetSchema} />}
    <div className="grid2 comparisonFilterGrid"><FilterEditor title="Source filters" schema={props.sourceSchema} filters={props.sourceFilters} setFilters={props.setSourceFilters} /><FilterEditor title="Target filters" schema={props.targetSchema} filters={props.targetFilters} setFilters={props.setTargetFilters} /></div>
    <IgnoredColumns sourceSchema={props.sourceSchema} targetSchema={props.targetSchema} ignoredSourceColumns={props.ignoredSourceColumns} setIgnoredSourceColumns={props.setIgnoredSourceColumns} ignoredTargetColumns={props.ignoredTargetColumns} setIgnoredTargetColumns={props.setIgnoredTargetColumns} />
    {(levels.includes("L5") || levels.includes("L6")) && <RuleSelection availableRules={props.availableRules} selectedDqRuleIds={props.selectedDqRuleIds} setSelectedDqRuleIds={props.setSelectedDqRuleIds} selectedAggRuleIds={props.selectedAggRuleIds} setSelectedAggRuleIds={props.setSelectedAggRuleIds} showDq={levels.includes("L6")} showAggregate={levels.includes("L5")} />}
  </div>;
}
