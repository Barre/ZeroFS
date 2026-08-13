"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const reconcile = require("../reconcile-update-ci-comment.js");

const context = {
  repo: { owner: "Barre", repo: "ZeroFS" },
};
const env = {
  PULL_NUMBER: "588",
  HEAD_SHA: "0123456789abcdef0123456789abcdef01234567",
  RUN_ID: "31650879787",
};
const canonicalBody =
  "<!-- kernel-target-update-ci -->\n" +
  "CI for [`0123456789ab`](https://github.com/Barre/ZeroFS/commit/" +
  "0123456789abcdef0123456789abcdef01234567): " +
  "[view checks](https://github.com/Barre/ZeroFS/actions/runs/" +
  "31650879787).";

function githubWith(comments) {
  const calls = [];
  const listComments = Symbol("issues.listComments");
  return {
    calls,
    listComments,
    github: {
      paginate: async (method, options) => {
        calls.push(["list", method, options]);
        return comments;
      },
      rest: {
        issues: {
          listComments,
          createComment: async options => calls.push(["create", options]),
          updateComment: async options => calls.push(["update", options]),
        },
      },
    },
  };
}

test("creates one CI link comment and ignores a spoofed marker", async () => {
  const fixture = githubWith([
    {
      id: 1,
      body: "<!-- kernel-target-update-ci -->",
      user: { login: "someone-else" },
    },
  ]);
  await reconcile({ github: fixture.github, context, env });

  assert.deepEqual(fixture.calls[0], [
    "list",
    fixture.listComments,
    {
      owner: "Barre",
      repo: "ZeroFS",
      issue_number: 588,
      per_page: 100,
    },
  ]);
  assert.deepEqual(fixture.calls[1], [
    "create",
    {
      owner: "Barre",
      repo: "ZeroFS",
      issue_number: 588,
      body: canonicalBody,
    },
  ]);
});

test("updates the existing bot-owned CI link comment", async () => {
  const fixture = githubWith([
    {
      id: 42,
      body: "<!-- kernel-target-update-ci -->\nstale",
      user: { login: "github-actions[bot]" },
    },
  ]);
  await reconcile({ github: fixture.github, context, env });

  assert.deepEqual(fixture.calls[1], [
    "update",
    {
      owner: "Barre",
      repo: "ZeroFS",
      comment_id: 42,
      body: canonicalBody,
    },
  ]);
});

test("leaves the canonical CI link comment unchanged", async () => {
  const fixture = githubWith([
    {
      id: 42,
      body: canonicalBody,
      user: { login: "github-actions[bot]" },
    },
  ]);
  await reconcile({ github: fixture.github, context, env });

  assert.equal(fixture.calls.length, 1);
});

test("rejects duplicate bot-owned CI link comments", async () => {
  const fixture = githubWith([
    {
      id: 42,
      body: "<!-- kernel-target-update-ci -->",
      user: { login: "github-actions[bot]" },
    },
    {
      id: 43,
      body: "<!-- kernel-target-update-ci -->",
      user: { login: "github-actions[bot]" },
    },
  ]);

  await assert.rejects(
    reconcile({ github: fixture.github, context, env }),
    /multiple CI link comments/,
  );
  assert.equal(fixture.calls.length, 1);
});

test("rejects a malformed run ID", async () => {
  const fixture = githubWith([]);

  await assert.rejects(
    reconcile({
      github: fixture.github,
      context,
      env: {
        ...env,
        RUN_ID: "not-a-run",
      },
    }),
    /invalid RUN_ID/,
  );
  assert.equal(fixture.calls.length, 0);
});

test("rejects malformed pull request and commit identities", async () => {
  const fixture = githubWith([]);

  await assert.rejects(
    reconcile({
      github: fixture.github,
      context,
      env: { ...env, PULL_NUMBER: "0" },
    }),
    /invalid PULL_NUMBER/,
  );
  await assert.rejects(
    reconcile({
      github: fixture.github,
      context,
      env: { ...env, HEAD_SHA: "not-a-commit" },
    }),
    /invalid HEAD_SHA/,
  );
  assert.equal(fixture.calls.length, 0);
});
