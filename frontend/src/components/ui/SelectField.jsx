import React from "react";
import Field from "./Field.jsx";

export default function SelectField({
  label,
  required = false,
  value,
  setValue,
  onChange,
  options = [],
  placeholder,
  disabled = false,
  className = "",
  ...props
}) {
  function handleChange(event) {
    if (setValue) setValue(event.target.value);
    if (onChange) onChange(event);
  }

  return (
    <Field label={label} required={required}>
      <select
        className={className}
        value={value ?? ""}
        onChange={handleChange}
        disabled={disabled}
        {...props}
      >
        {placeholder !== undefined && <option value="">{placeholder}</option>}
        {options.map((option) => {
          const normalized = Array.isArray(option)
            ? { value: option[0], label: option[1] }
            : typeof option === "object"
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
