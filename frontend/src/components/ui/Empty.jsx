import React from "react";

export default function Empty({ icon: Icon, title, text }) {
  return (
    <div className="empty">
      <Icon size={22} />
      <b>{title}</b>
      <span>{text}</span>
    </div>
  );
}
