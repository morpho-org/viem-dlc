import { QuoteLens } from "@/lens";

export default function Page() {
  return (
    <main>
      <h1>{QuoteLens.name}</h1>
      <code>{QuoteLens.bytecode}</code>
    </main>
  );
}
