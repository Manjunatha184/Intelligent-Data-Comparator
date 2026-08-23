import React from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/montserrat/latin-400.css";
import "@fontsource/montserrat/latin-500.css";
import "@fontsource/montserrat/latin-600.css";
import "@fontsource/montserrat/latin-700.css";

// Comparison step 2 can create DQ/Aggregate rules inline. Register its rule
// editor before the application is rendered so the existing RuleSelectionModal
// can open it without crashing the React tree.
import "./features/rules/ComparisonRuleModalGlobal";
import App from "./App";

// CSS ownership:
// 1. styles.css          -> base/global component styles
// 2. enterprise-theme.css -> Lumera visual theme
// 3. lumera-ui.css       -> feature/page presentation modules in cascade order
import "./styles.css";
import "./enterprise-theme.css";
import "./lumera-ui.css";

createRoot(document.getElementById("root")).render(<App />);
