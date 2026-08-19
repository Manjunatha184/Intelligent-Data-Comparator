import React from "react";
import Field from "./Field.jsx";

export default function SelectField({
  label,
  required = false,
  value,
  onChange,
  options = [],
  placeholder,
  disabled = false,
  className = "",
  ...props
}) {
  return (
    <Field label={label} required={required}>
      <select
        className={className}
        value={value ?? ""}
        onChange={onChange}
        disabled={disabled}
        {...props}
      >
        {placeholder !== undefined && <option value="">{placeholder}</option>}
        {options.map((option) => {
          const normalized =
            typeof option === "object"
              ? option
              : { value: option, label: option };
          return (
            <option
              key={String(normalized.value)}
              value={normalized.value}
              disabled={Boolean(normalized.disabled)}
            >
              {normalized.label}
            </option>
          );
        })}
      </select>
    </Field>
  );
}
