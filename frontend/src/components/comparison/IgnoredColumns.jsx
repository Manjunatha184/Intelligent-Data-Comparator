import React from "react";
import { getSchemaColumnNames } from "../../utils/schema.js";
import MultiSelectField from "../ui/MultiSelectField.jsx";

export default function IgnoredColumns({ sourceSchema, targetSchema, ignoredSourceColumns, setIgnoredSourceColumns, ignoredTargetColumns, setIgnoredTargetColumns }) {
  return (
    <section className="comparisonSection">
      <div className="comparisonSectionHead"><div><h3>Ignored columns</h3><p className="helper">Exclude fields that should not participate in comparison.</p></div></div>
      <div className="grid2">
        <div className="field"><span>Source columns</span><MultiSelectField options={getSchemaColumnNames(sourceSchema)} selected={ignoredSourceColumns} onChange={setIgnoredSourceColumns} placeholder="Select source columns" /></div>
        <div className="field"><span>Target columns</span><MultiSelectField options={getSchemaColumnNames(targetSchema)} selected={ignoredTargetColumns} onChange={setIgnoredTargetColumns} placeholder="Select target columns" /></div>
      </div>
    </section>
  );
}
