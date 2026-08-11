const title = "Update distro kernel package targets";
const body =
  "Automated kernel lock update. CI builds and boots every retained target.";

module.exports = async ({ github, context, env = process.env }) => {
  const owner = context.repo.owner;
  const repo = context.repo.repo;
  const branch = env.UPDATE_BRANCH?.trim();
  if (!branch) {
    throw new Error("UPDATE_BRANCH must not be blank");
  }
  if (env.CHANGED !== "true" && env.CHANGED !== "false") {
    throw new Error("CHANGED must be exactly true or false");
  }
  const pulls = await github.paginate(github.rest.pulls.list, {
    owner,
    repo,
    state: "open",
    head: `${owner}:${branch}`,
    base: context.payload.repository.default_branch,
    per_page: 100,
  });
  if (pulls.length > 1) {
    throw new Error(`multiple update PRs use ${branch}`);
  }

  const pull = pulls[0];
  if (env.CHANGED === "false") {
    if (pull) {
      await github.rest.pulls.update({
        owner,
        repo,
        pull_number: pull.number,
        state: "closed",
      });
    }
    return;
  }

  if (pull) {
    if (pull.title !== title || pull.body !== body) {
      await github.rest.pulls.update({
        owner,
        repo,
        pull_number: pull.number,
        title,
        body,
      });
    }
  } else {
    await github.rest.pulls.create({
      owner,
      repo,
      head: branch,
      base: context.payload.repository.default_branch,
      title,
      body,
    });
  }
};
