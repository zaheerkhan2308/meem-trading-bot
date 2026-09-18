import { defineConfig } from "@neon/config/v1";

export default defineConfig({
  preview: {
    buckets: {
      logs: { access: "private" },
    },
  },
});
