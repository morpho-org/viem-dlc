import ActionListItem from "@components/ActionListItem";
import SidebarLayout from "@components/SidebarLayout";
import * as React from "react";

import { EXAMPLES, exampleById } from "./examples/index.js";
import { About } from "./pages/About.js";
import { ExamplePage } from "./pages/ExamplePage.js";
import { navigate, useRoute } from "./router.js";

const ITEMS = [
  { id: "about", title: "about", group: "" },
  ...EXAMPLES.map((example) => ({ id: example.id, title: example.title, group: "examples" })),
];

export function App() {
  const route = useRoute();
  const example = exampleById(route);
  // The RPC endpoint is a property of the session, not of a page, so it survives navigation.
  const [rpcUrl, setRpcUrl] = React.useState("https://mainnet.base.org");

  const sidebar = (
    <nav className="sidebar">
      <h1 className="brand">viem-dlc</h1>
      {ITEMS.map((item, index) => (
        <React.Fragment key={item.id}>
          {item.group && ITEMS[index - 1]?.group !== item.group ? <div className="group">{item.group}</div> : null}
          <ActionListItem
            icon={route === item.id ? "▸" : " "}
            onClick={() => navigate(item.id)}
            // ActionListItem exposes no selected state, only `style`.
            style={route === item.id ? { background: "var(--theme-focused-foreground)" } : undefined}
          >
            {item.title}
          </ActionListItem>
        </React.Fragment>
      ))}
      <div className="group">links</div>
      <ActionListItem icon="⭢" href="https://github.com/morpho-org/viem-dlc" target="_blank">
        github
      </ActionListItem>
    </nav>
  );

  return (
    <SidebarLayout defaultSidebarWidth={24} isShowingHandle sidebar={sidebar}>
      <div className="page">
        {example ? (
          <ExamplePage example={example} rpcUrl={rpcUrl} onRpcUrl={setRpcUrl} />
        ) : route === "about" ? (
          <About />
        ) : (
          <About />
        )}
      </div>
    </SidebarLayout>
  );
}
