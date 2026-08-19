import React from "react";
import { X } from "lucide-react";

export default function Modal({ title, onClose, children, footer, className = "" }) {
  return (
    <div className="modalBackdrop" role="presentation">
      <section className={`modal ${className}`.trim()} role="dialog" aria-modal="true" aria-label={title}>
        <header className="modalHead">
          <h2>{title}</h2>
          <button type="button" className="iconButton" onClick={onClose} aria-label="Close">
            <X size={18} />
          </button>
        </header>
        <div className="modalBody">{children}</div>
        {footer && <footer className="modalFooter">{footer}</footer>}
      </section>
    </div>
  );
}
