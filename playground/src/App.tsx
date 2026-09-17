import { Button, Flex, Heading, Text, Theme } from "@radix-ui/themes";
import * as React from "react";

import { About } from "./pages/About.js";
import { TutorialPage } from "./pages/TutorialPage.js";
import { navigate, useRoute } from "./router.js";
import { TUTORIALS, tutorialById } from "./tutorials/index.js";

const ITEMS = [
  { id: "about", title: "About", group: "" },
  ...TUTORIALS.map((tutorial) => ({ id: tutorial.id, title: tutorial.title, group: "tutorials" })),
];

function useSystemAppearance(): "light" | "dark" {
  const [dark, setDark] = React.useState(() => window.matchMedia("(prefers-color-scheme: dark)").matches);

  React.useEffect(() => {
    const query = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => setDark(query.matches);
    query.addEventListener("change", apply);
    return () => query.removeEventListener("change", apply);
  }, []);

  return dark ? "dark" : "light";
}

function GroupLabel({ children }: { children: React.ReactNode }) {
  return (
    <Text as="div" size="1" color="gray" className="group">
      {children}
    </Text>
  );
}

export function App() {
  const appearance = useSystemAppearance();
  const route = useRoute();
  const tutorial = tutorialById(route);
  // The RPC endpoint is a property of the session, not of a page, so it survives navigation.
  const [rpcUrl, setRpcUrl] = React.useState("https://mainnet.base.org");

  return (
    <Theme
      appearance={appearance}
      accentColor="gray"
      grayColor="sand"
      radius="none"
      scaling="95%"
      panelBackground="solid"
    >
      <div className="shell">
        <Flex asChild direction="column" gap="1" p="4" className="sidebar">
          <nav>
            <Heading size="2" mb="2" className="brand">
              viem-dlc
            </Heading>
            {ITEMS.map((item, index) => (
              <React.Fragment key={item.id}>
                {item.group && ITEMS[index - 1]?.group !== item.group ? <GroupLabel>{item.group}</GroupLabel> : null}
                <Button
                  variant="soft"
                  color="gray"
                  className={route === item.id ? "nav-item nav-item-active" : "nav-item"}
                  onClick={() => navigate(item.id)}
                >
                  {item.title}
                </Button>
              </React.Fragment>
            ))}
            <GroupLabel>links</GroupLabel>
            <Button asChild variant="soft" color="gray" className="nav-item">
              <a href="https://github.com/morpho-org/viem-dlc" target="_blank" rel="noreferrer">
                github ↗
              </a>
            </Button>
          </nav>
        </Flex>

        <main className="page">
          {tutorial ? <TutorialPage tutorial={tutorial} rpcUrl={rpcUrl} onRpcUrl={setRpcUrl} /> : <About />}
        </main>
      </div>
    </Theme>
  );
}
