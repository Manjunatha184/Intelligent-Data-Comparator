import React, { useEffect, useRef, useState } from "react";

export default function MultiSelectField({ options = [], selected = [], onChange, placeholder }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  useEffect(() => {
    function close(event) {
      if (rootRef.current && !rootRef.current.contains(event.target)) setOpen(false);
    }
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);

  function toggle(value) {
    onChange(selected.includes(value) ? selected.filter((item) => item !== value) : [...selected, value]);
  }

  return (
    <div className="multiSelectField" ref={rootRef}>
      <button type="button" className="multiSelectTrigger" aria-expanded={open} onClick={() => setOpen(!open)}>
        <span>{placeholder}</span><span className="multiSelectChevron">{open ? "▲" : "▼"}</span>
      </button>
      {open && <div className="multiSelectMenu" role="listbox">
        {options.map((option) => <button type="button" role="option" aria-selected={selected.includes(option)} className="multiSelectOption" key={option} onClick={() => toggle(option)}><span className="multiSelectCheck">{selected.includes(option) ? "✓" : ""}</span>{option}</button>)}
        {!options.length && <span className="multiSelectEmpty">No schema fields available</span>}
      </div>}
    </div>
  );
}
