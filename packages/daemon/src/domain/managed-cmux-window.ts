import {shellQuote} from "../adapters/shell-quote.js";
/** Per-window only. Explicit opt-out is durable and readable by the web broker. */
export function managedCmuxCommand(session:string,attachCommand:string):string {
 const target=shellQuote(session);
 return `openrig_cmux_sizing=$(tmux show-options -wqv -t ${target} @openrig-preserve-window-size) && { if [ "$openrig_cmux_sizing" != "1" ]; then tmux set-option -w -t ${target} @openrig-cmux-auto-size 1 && tmux set-option -w -t ${target} fill-character ' ' && tmux set-option -w -t ${target} window-size latest; fi; } && ${attachCommand}`;
}

/** Stable role labels for managed seats; unknown identities remain readable. */
export function managedSeatTitle(seat:string):string {
 const id=seat.split("@")[0]!.replace(/-/g,".").toLowerCase();
 if(/space.?bunny/.test(id))return "Reviewer Space Bunny";
 if(/dgx|qwen/.test(id))return "Reviewer DGX";
 if(/queue.*worker/.test(id))return "Queue Worker";
 const role=id.split(".").at(-1)!;
 const titles:Record<string,string>={lead:"Lead",peer:"Peer",architect:"Architect",advisor:"Advisor",impl:"Builder Sol",impl2:"Builder Luna",sol:"Builder Sol",luna:"Builder Luna",design:"Designer",designer:"Designer",qa:"QA",r1:"Reviewer GLM",r2:"Reviewer Sonnet",r3:"Reviewer DeepSeek",glm:"Reviewer GLM",sonnet:"Reviewer Sonnet",deepseek:"Reviewer DeepSeek",human:"Dashboard"};
 if(/operator.*agent/.test(id))return "Operator";
 return titles[role]??role.replace(/(^|[._])([a-z])/g,(_m,_a,b:string)=>` ${b.toUpperCase()}`).trim();
}
