import React from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/montserrat/latin-400.css";
import "@fontsource/montserrat/latin-500.css";
import "@fontsource/montserrat/latin-600.css";
import "@fontsource/montserrat/latin-700.css";

import App from "./App";
import "./styles.css";
import "./enterprise-theme.css";
import "./comparison-reference.css";
import "./comparison-polish.css";
import "./page-header.css";
import "./layout-consistency.css";
import "./comparison-step2.css";

createRoot(document.getElementById("root")).render(<App />);