/** A current native update menu, not an update banner in conversation or scrollback.
 * Recognition grants no input permission; callers retain process identity checks.
 */
export function isCodexUpdateMenu(paneContent: string): boolean {
  // A later live TUI supersedes an old menu above its header.
  const current = paneContent.slice(Math.max(0, paneContent.lastIndexOf("OpenAI Codex (v")));
  const header = /^[ \t]*(?:✨[ \t]*)?Update available(?:!(?:[ \t]+\d[\w.-]*[ \t]+(?:->|→)[ \t]+\d[\w.-]*)?|[ \t]+·[ \t]+\d[\w.-]*[ \t]+→[ \t]+\d[\w.-]*)[ \t]*$/m;
  return header.test(current)
    && /^[ \t]*[›>]?[ \t]*1\. Update now \(runs `npm install -g @openai\/codex`\)[ \t]*$/m.test(current)
    && /^[ \t]*[›>]?[ \t]*2\. Skip[ \t]*$/m.test(current)
    && /^[ \t]*[›>]?[ \t]*3\. Skip until next version[ \t]*$/m.test(current);
}
