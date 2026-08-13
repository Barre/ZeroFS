"use strict";

const marker = "<!-- kernel-target-update-ci -->";

function positiveInteger(text, name) {
  if (!/^[1-9][0-9]*$/.test(text)) {
    throw new Error(`invalid ${name}: ${text}`);
  }
  const value = Number(text);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`invalid ${name}: ${text}`);
  }
  return value;
}

module.exports = async ({ github, context, env = process.env }) => {
  const owner = context.repo.owner;
  const repo = context.repo.repo;
  const pullNumberText = env.PULL_NUMBER?.trim() ?? "";
  const headSha = env.HEAD_SHA?.trim() ?? "";
  const runIdText = env.RUN_ID?.trim() ?? "";

  const pullNumber = positiveInteger(pullNumberText, "PULL_NUMBER");
  const runId = positiveInteger(runIdText, "RUN_ID");
  if (!/^[0-9a-f]{40}$/.test(headSha)) {
    throw new Error(`invalid HEAD_SHA: ${headSha}`);
  }

  const commitUrl = `https://github.com/${owner}/${repo}/commit/${headSha}`;
  const runUrl = `https://github.com/${owner}/${repo}/actions/runs/${runId}`;
  const body = [
    marker,
    `CI for [\`${headSha.slice(0, 12)}\`](${commitUrl}): ` +
      `[view checks](${runUrl}).`,
  ].join("\n");
  const comments = await github.paginate(github.rest.issues.listComments, {
    owner,
    repo,
    issue_number: pullNumber,
    per_page: 100,
  });
  const owned = comments.filter(
    comment =>
      comment.user?.login === "github-actions[bot]" &&
      comment.body?.startsWith(marker),
  );
  if (owned.length > 1) {
    throw new Error(`multiple CI link comments exist on PR #${pullNumber}`);
  }

  if (owned.length === 0) {
    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: pullNumber,
      body,
    });
    return;
  }
  if (owned[0].body === body) {
    return;
  }
  await github.rest.issues.updateComment({
    owner,
    repo,
    comment_id: owned[0].id,
    body,
  });
};
