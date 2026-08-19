from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"
MONOLITH = SRC / "legacy" / "MonolithApp.jsx"
lines = MONOLITH.read_text(encoding="utf-8").splitlines()


def block(start, end):
    return "\n".join(lines[start - 1:end]) + "\n"


def write(path, content):
    target = SRC / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


# Real application shell. Keep routing/state identical to the working monolith.
app = block(366, 551).replace("function App()", "export default function App()", 1)
app = app.replace("<ConnectionManager", "<Connections").replace("</ConnectionManager>", "</Connections>")
app = app.replace("<RulesPage", "<Rules").replace("</RulesPage>", "</Rules>")
write("App.jsx", '''import React, { useEffect, useState } from "react";
import { apiRequest } from "./api/client.js";
import Sidebar from "./components/layout/Sidebar.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Connections from "./pages/Connections.jsx";
import ConnectionModal from "./components/connections/ConnectionModal.jsx";
import ComparisonBuilder from "./components/comparison/ComparisonBuilder.jsx";
import Results from "./pages/Results.jsx";
import AnalysisPage from "./pages/Analysis.jsx";
import Rules from "./pages/Rules.jsx";
import Toast from "./components/ui/Toast.jsx";

''' + app)

write("main.jsx", '''import React from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/montserrat/latin-400.css";
import "@fontsource/montserrat/latin-500.css";
import "@fontsource/montserrat/latin-600.css";
import "@fontsource/montserrat/latin-700.css";
import "./styles.css";
import "./ui-normalization.css";
import App from "./App.jsx";

createRoot(document.getElementById("root")).render(<App />);
''')

# Comparison orchestration: preserve payload creation and API flow exactly.
builder = block(1409, 2067)
builder = builder.replace("function ComparisonBuilder(", "export default function ComparisonBuilder(", 1)
builder = builder.replace("<SourceStep", "<SourceTargetStep").replace("</SourceStep>", "</SourceTargetStep>")
builder = builder.replace("<LevelsStep", "<LevelSelectionStep").replace("</LevelsStep>", "</LevelSelectionStep>")
builder = builder.replace("<RulesStep", "<RulesConfigurationStep").replace("</RulesStep>", "</RulesConfigurationStep>")
builder = builder.replace("/* ============================================================\n   COMPARISON SOURCES\n", "")
builder += '\nconst NULL_FILTER_OPERATORS = new Set(["IS NULL", "IS NOT NULL"]);\n\n' + block(2906, 2914)
write("components/comparison/ComparisonBuilder.jsx", '''import React, { useEffect, useState } from "react";
import { ArrowRight } from "lucide-react";
import { apiRequest } from "../../api/client.js";
import { defaultMappedPair, getSchemaColumnNames, isNumericMapping, rowsEqual } from "../../utils/schema.js";
import { normalizeAggregateRulePayload, normalizeDqRulePayload } from "../../utils/rules.js";
import SourceTargetStep from "./SourceTargetStep.jsx";
import LevelSelectionStep from "./LevelSelectionStep.jsx";
import RulesConfigurationStep from "./RulesConfigurationStep.jsx";
import ReviewModal from "./ReviewModal.jsx";

''' + builder)

# Exact configuration UI from the working source, including normalization,
# grouped reconciliation, filters, ignored columns, and rule selection.
rules = block(2390, 2944).replace("function RulesStep(", "export default function RulesConfigurationStep(", 1)
write("components/comparison/RulesConfigurationStep.jsx", '''import React from "react";
import { Plus, Trash2, X } from "lucide-react";
import { getSchemaColumnNames, isNumericMapping } from "../../utils/schema.js";
import Panel from "../ui/Panel.jsx";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";
import RuleModal from "../rules/RuleModal.jsx";

''' + rules)

review = block(2945, 3046).replace("function ReviewModal(", "export default function ReviewModal(", 1) + "\n" + block(3047, 3072)
write("components/comparison/ReviewModal.jsx", '''import React from "react";
import { Loader2, X, Zap } from "lucide-react";
import Panel from "../ui/Panel.jsx";

''' + review)

# Preserve full Results evidence/report rendering.
results = block(3073, 4421)
results = results.replace("function Results(", "export default function Results(", 1)
results = results.replace("function L7AnalysisReportView(", "export function L7AnalysisReportView(", 1)
write("pages/Results.jsx", '''import React, { useEffect, useState } from "react";
import { Check, ChevronRight, Download, FileText, Loader2, RefreshCw, Trash2, TriangleAlert, X } from "lucide-react";
import { apiRequest } from "../api/client.js";
import Empty from "../components/ui/Empty.jsx";
import Loading from "../components/ui/Loading.jsx";
import Panel from "../components/ui/Panel.jsx";
import Status from "../components/ui/Status.jsx";

''' + results)

analysis = block(314, 360).replace("function AnalysisPage(", "export default function AnalysisPage(", 1)
write("pages/Analysis.jsx", '''import React, { useEffect, useState } from "react";
import { API_BASE, apiRequest } from "../api/client.js";
import Loading from "../components/ui/Loading.jsx";
import { L7AnalysisReportView } from "./Results.jsx";

''' + analysis)

print("Frontend modules generated from legacy/MonolithApp.jsx")
