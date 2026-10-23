// Marker comments belong to the current GitHub App, never a human lookalike.
module.exports = async function statusComment({github, core}, env = process.env) {
  const {STATUS_REPOSITORY, STATUS_PR, STATUS_MARKER, STATUS_BODY, STATUS_MODE, STATUS_APP_SLUG} = env;
  if (!STATUS_PR || STATUS_PR === 'null' || STATUS_PR === '0') return;
  if (!/^[^/]+\/[^/]+$/.test(STATUS_REPOSITORY) || !/^[1-9][0-9]*$/.test(STATUS_PR)) {
    throw new Error('A repository and positive PR number are required');
  }
  if (!/^<!-- [a-zA-Z0-9_-]+ -->$/.test(STATUS_MARKER)) throw new Error('Invalid status marker');
  if (!['upsert', 'replace', 'purge'].includes(STATUS_MODE)) throw new Error('Invalid comment mode');
  const [owner, repo] = STATUS_REPOSITORY.split('/');
  const issue_number = Number(STATUS_PR);
  if (!STATUS_APP_SLUG) throw new Error('GitHub App slug is required');
  const comments = await github.paginate(github.rest.issues.listComments,
    {owner, repo, issue_number, per_page: 100});
  const owned = comments.filter(c => c.performed_via_github_app?.slug === STATUS_APP_SLUG &&
    (c.body || '').split('\n')[0] === STATUS_MARKER);
  let selected = STATUS_MODE === 'upsert' ? owned.shift() : undefined;
  for (const comment of owned) await github.rest.issues.deleteComment({owner, repo, comment_id: comment.id});
  if (STATUS_MODE === 'purge') return;
  const body = `${STATUS_MARKER}\n${STATUS_BODY}`;
  let result;
  if (selected) {
    result = await github.rest.issues.updateComment({owner, repo, comment_id: selected.id, body});
  } else {
    result = await github.rest.issues.createComment({owner, repo, issue_number, body});
  }
  core.setOutput('comment-id', result.data.id);
};
