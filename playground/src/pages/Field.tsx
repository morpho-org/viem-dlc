import { Text, TextField } from "@radix-ui/themes";
import * as React from "react";

import type { Control } from "../tutorials/types.js";

export function Field({
  label,
  value,
  type,
  onChange,
}: {
  label: string;
  value: string;
  type?: Control["type"];
  onChange: (value: string) => void;
}) {
  const id = React.useId();

  return (
    <div>
      <Text as="label" htmlFor={id} size="1" color="gray" mb="1" className="field-label">
        {label}
      </Text>
      <TextField.Root
        id={id}
        value={value}
        type={type === "number" ? "number" : "text"}
        spellCheck={false}
        onChange={(event) => onChange(event.target.value)}
      />
    </div>
  );
}
