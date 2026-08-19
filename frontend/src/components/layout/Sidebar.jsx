import React from "react";
import {
  Activity,
  ChevronRight,
  GitCompare,
  LayoutDashboard,
  Link2,
  SlidersHorizontal,
} from "lucide-react";

function NavigationItem({ icon: Icon, label, active, disabled, onClick }) {
  return (
    <button
      className={`nav ${active ? "active" : ""}`}
      disabled={disabled}
      onClick={onClick}
    >
      <Icon size={17} />
      <span>{label}</span>
      {active && <ChevronRight size={14} />}
    </button>
  );
}

export default function Sidebar({ page, setPage, onOpenResultsHistory }) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brandWordmark">DATA COMPARATOR</div>
      </div>

      <div className="workspace">WORKSPACE</div>

      <NavigationItem
        icon={LayoutDashboard}
        label="Overview"
        active={page === "dashboard"}
        onClick={() => setPage("dashboard")}
      />
      <NavigationItem
        icon={GitCompare}
        label="Comparisons"
        active={page === "comparison"}
        onClick={() => setPage("comparison")}
      />
      <NavigationItem
        icon={Activity}
        label="Results"
        active={page === "results"}
        onClick={onOpenResultsHistory}
      />

      <div className="workspace">CONFIGURATION</div>

      <NavigationItem
        icon={Link2}
        label="Connection Manager"
        active={page === "connections"}
        onClick={() => setPage("connections")}
      />
      <NavigationItem
        icon={SlidersHorizontal}
        label="Rule Repository"
        active={page === "rules"}
        onClick={() => setPage("rules")}
      />

      <div className="sidebarBottom">
        <div className="apiState">
          <i />
          <div>
            <b>Backend online</b>
            <span>FastAPI · :8000</span>
          </div>
        </div>
      </div>
    </aside>
  );
}
