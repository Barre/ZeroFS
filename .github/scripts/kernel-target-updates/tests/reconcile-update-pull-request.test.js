"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const reconcile = require("../reconcile-update-pull-request.js");

const context = {
  repo: { owner: "Barre", repo: "ZeroFS" },
  payload: { repository: { default_branch: "main" } },
};

const canonicalBody =
  "Automated kernel lock update. CI builds and boots every retained target.";

function githubWith(pulls) {
  const calls = [];
  const list = Symbol("pulls.list");
  return {
    calls,
    github: {
      paginate: async (method, options) => {
        calls.push(["list", method, options]);
        return pulls;
      },
      rest: {
        pulls: {
          list,
          create: async options => calls.push(["create", options]),
          update: async options => calls.push(["update", options]),
        },
      },
    },
    list,
  };
}

const changedEnv = {
  UPDATE_BRANCH: "automation/kernel-target-updates",
  CHANGED: "true",
};

test("creates an update pull request", async () => {
  const fixture = githubWith([]);
  await reconcile({ github: fixture.github, context, env: changedEnv });

  assert.equal(fixture.calls[0][0], "list");
  assert.equal(fixture.calls[0][1], fixture.list);
  assert.deepEqual(fixture.calls[1], [
    "create",
    {
      owner: "Barre",
      repo: "ZeroFS",
      head: changedEnv.UPDATE_BRANCH,
      base: "main",
      title: "Update distro kernel package targets",
      body: canonicalBody,
    },
  ]);
});

test("updates a noncanonical existing update pull request", async () => {
  const fixture = githubWith([
    { number: 567, title: "Old title", body: "Old body" },
  ]);
  await reconcile({ github: fixture.github, context, env: changedEnv });

  assert.deepEqual(fixture.calls[1], [
    "update",
    {
      owner: "Barre",
      repo: "ZeroFS",
      pull_number: 567,
      title: "Update distro kernel package targets",
      body: canonicalBody,
    },
  ]);
});

test("leaves a canonical existing update pull request unchanged", async () => {
  const fixture = githubWith([
    {
      number: 567,
      title: "Update distro kernel package targets",
      body: canonicalBody,
    },
  ]);
  await reconcile({ github: fixture.github, context, env: changedEnv });

  assert.equal(fixture.calls.length, 1);
  assert.equal(fixture.calls[0][0], "list");
});

test("closes the existing update pull request when nothing changed", async () => {
  const fixture = githubWith([{ number: 567 }]);
  await reconcile({
    github: fixture.github,
    context,
    env: {
      UPDATE_BRANCH: changedEnv.UPDATE_BRANCH,
      CHANGED: "false",
    },
  });

  assert.deepEqual(fixture.calls[1], [
    "update",
    {
      owner: "Barre",
      repo: "ZeroFS",
      pull_number: 567,
      state: "closed",
    },
  ]);
});

test("rejects duplicate update pull requests", async () => {
  const fixture = githubWith([{ number: 1 }, { number: 2 }]);
  await assert.rejects(
    reconcile({ github: fixture.github, context, env: changedEnv }),
    /multiple update PRs/,
  );
});
