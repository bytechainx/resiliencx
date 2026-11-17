#!/usr/bin/env node
// E1 收据生成：把 CI 平台注入的流水线/作业/提交身份绑定成一份收据 JSON。
// 只在 CI 内调用；缺少任一必需绑定字段即退出 2，不产出残缺收据。
// 用法：node scripts/emit-e1-receipt.mjs --out <path>

import fs from "node:fs";
import path from "node:path";

const USAGE = "用法: node scripts/emit-e1-receipt.mjs --out <path>";

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

const out = argValue("--out");
if (!out) {
  console.error(`工具错误: 缺少 --out\n${USAGE}`);
  process.exit(2);
}

const env = process.env;
const isGitHub = Boolean(env.GITHUB_ACTIONS);
const isGitLab = Boolean(env.GITLAB_CI);
if (!isGitHub && !isGitLab) {
  console.error("工具错误: 未检测到 CI 环境（GITHUB_ACTIONS / GITLAB_CI 均未设置），E1 收据只在 CI 内生成");
  process.exit(2);
}

const commitSha = env.GITHUB_SHA || env.CI_COMMIT_SHA || "";
const pipelineId = env.GITHUB_RUN_ID || env.CI_PIPELINE_ID || "";
const jobId = isGitHub ? `github-actions:${env.GITHUB_JOB || "resiliencx-prod-live"}` : `gitlab-ci:${env.CI_JOB_ID || "resiliencx-prod-live"}`;

if (!/^[0-9a-f]{40}$/.test(commitSha)) {
  console.error(`工具错误: commit_sha 缺失或非法: ${commitSha || "(空)"}`);
  process.exit(2);
}
if (!pipelineId) {
  console.error("工具错误: pipeline id 缺失（GITHUB_RUN_ID / CI_PIPELINE_ID）");
  process.exit(2);
}

const receipt = {
  schema_version: 1,
  kind: "e1-live-receipt",
  crate: "resiliencx",
  trust_level: "E1",
  result: "pass",
  commit_sha: commitSha,
  pipeline: {
    id: String(pipelineId),
    attempt: env.GITHUB_RUN_ATTEMPT || env.CI_PIPELINE_IID || null,
    url: isGitHub
      ? `${env.GITHUB_SERVER_URL}/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}`
      : env.CI_PIPELINE_URL || null,
  },
  job: {
    id: jobId,
    name: "resiliencx-prod-live",
    url: isGitLab ? env.CI_JOB_URL || null : null,
  },
  generated_at: new Date().toISOString(),
};

fs.mkdirSync(path.dirname(path.resolve(out)), { recursive: true });
fs.writeFileSync(out, JSON.stringify(receipt, null, 2) + "\n");
console.log(`E1 收据已生成: ${out} (commit=${commitSha.slice(0, 12)} pipeline=${pipelineId})`);
