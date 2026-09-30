#!/usr/bin/env python3
"""Build the failure-campaign report (figures/final/aqraft_failure_campaign.html) from
figures/final/metrics_final.json, the tput_*/gantt_* figures and the per-scenario text below."""
import base64, json, os, sys, html

here = sys.argv[1] if len(sys.argv) > 1 else "."
FIG = os.path.join(here, "figures", "final")
M = json.load(open(os.path.join(FIG, "metrics_final.json")))

def img(name, alt):
    p = os.path.join(FIG, name)
    if not os.path.exists(p):
        return f'<p class="note">Figure not available ({html.escape(name)}).</p>'
    b = base64.b64encode(open(p, "rb").read()).decode()
    return f'<img src="data:image/png;base64,{b}" alt="{html.escape(alt)}" loading="lazy">'

# Per-scenario text: what is killed, when, what should happen, and what the logs show.
S = [
 dict(id="healthy", tag="HEALTHY", title="No failure", role="reference",
      killed="nothing", when="—",
      expect="The reference migration: three donors move 1,365 slots each to sg4.",
      steps=["Each donor adopts its startup-registered source pool; the recipient answers every REGISTER-BLOCK-SLOTS from a pre-registered landing pool.",
             "Donors are dispatched one after another; each transfer takes about 1 s.",
             "Each batch is chain-forwarded, acked by a follower on its AppendEntries replies, merged, and committed (INDX_UPD, RECP_TXN_DONE, TXN_DONE)."]),
 dict(id="s1", tag="S1", title="Recipient leader crashes mid-transfer", role="recipient leader",
      killed="redis3 (sg4 leader)", when="on the 6th DONE-SLOTS-CHUNK, during sg2's transfer as sg3 is dispatched",
      expect="A follower becomes sg4 leader; every donor range reaches it (re-home for the in-flight session, re-drive for the rest).",
      steps=["redis3 dies 0.5 s after sg1's range commits, while sg2 is transferring and sg3 has just been dispatched.",
             "redis4 becomes sg4 leader 1.9 s later. Its become-leader hook finds the in-flight sessions and asks their donors to re-home to it.",
             "sg2 re-ships to redis4 over a chain that drops the dead redis3 at setup; sg2 is durable 26.5 s after the kill.",
             "sg3's dispatch to the dead redis3 fails after an 8 s REGISTER timeout; the playbook re-drives it once the round ends.",
             "The new leader has no record of sg1's batch (it lived on redis3), so the re-drive re-sends sg1 as well; the merge skips keys it already has.",
             "All three ranges are durable 72.1 s after the kill. Most of that is waiting for the round to end before re-driving (see open issues). Clients see 127 update errors and 23 s below half throughput."]),
 dict(id="s2", tag="S2", title="Donor leader crashes during its transfer", role="donor leader + orchestrator",
      killed="redis0 (sg1 leader, also runs the orchestrator)", when="on sg1's first DONE-SLOTS-CHUNK",
      expect="The new sg1 leader resumes sg1's session; the playbook re-drives the donors the dead orchestrator never dispatched.",
      steps=["redis0 dies 0.55 s after sg1's first chunk lands; the orchestrator dies with it, before sg2 and sg3 are dispatched.",
             "redis2 wins sg1's election 1.1 s later. Its become-leader hook finds sg1's session started but not done and calls MGN-RECOVER.",
             "The recipient reports none of sg1's slots durable yet, so the new leader re-ships all 1,365 slots to redis3.",
             "The playbook re-drives sg2 and sg3 from their own leaders (recovery ids 702 and 703).",
             "All three ranges are durable 16.5 s after the kill. Clients see no errors; throughput is below half for 4 s."]),
 dict(id="s3", tag="S3", title="Donor follower crashes", role="donor follower",
      killed="sg1's replica on redis1", when="on sg1's first DONE-SLOTS-CHUNK",
      expect="Nothing to recover: sg1 keeps 2 of 3 nodes and the transfer never uses followers.",
      steps=["No leader change and no recovery action in any log.",
             "All three sessions committed as in HEALTHY."]),
 dict(id="s4", tag="S4", title="Recipient follower crashes", role="recipient follower",
      killed="redis4 (first node in the chain)", when="as the leader starts forwarding the first batch to it",
      expect="The leader drops redis4 from the chain, forwards to redis5, and waits for redis5's real ack.",
      steps=["The forward to redis4 fails at once (Connection reset by peer).",
             "The leader re-forms the chain around redis4 and re-forwards straight to redis5; the next sessions drop redis4 when their chains are set up.",
             "redis5 reports each batch on its AppendEntries replies; each report becomes the ack that lets INDX_UPD commit.",
             "Clients stall for about 2 s while redis5, now sg4's only follower, takes over the chain role (see open issues)."]),
 dict(id="s5", tag="S5", title="Donor leader crashes after its transfer", role="donor leader + orchestrator",
      killed="redis0 (sg1 leader, also runs the orchestrator)", when="right after sg1's worker logs DONE",
      expect="Nothing is lost; a new sg1 leader takes over and the playbook confirms every range is durable.",
      steps=["The orchestrator dies with redis0; sg2 and sg3 had already been dispatched.",
             "The playbook's re-drive check finds sg1 and sg2 durable and sg3 pending; 2 s later sg3 is durable too, so nothing is re-sent."]),
 dict(id="s6", tag="S6", title="Donor and recipient leaders crash together", role="both leaders",
      killed="redis0 (sg1 leader) and redis3 (sg4 leader), 0.42 s apart", when="on the 6th DONE-SLOTS-CHUNK",
      expect="Both groups elect new leaders; every donor range is re-sent to the new sg4 leader from the current donor leaders.",
      steps=["sg1 elects redis2 and sg4 elects redis5, about 1 s after the kills.",
             "redis5 finds sg2's in-flight session and re-homes it; sg2's donor re-ships it to redis5.",
             "The playbook re-drives sg1 (from redis2) and sg3 to redis5; each new chain drops the dead redis3 at setup.",
             "The last range commits 16.5 s after the first kill; the playbook confirms all three durable at 18.3 s. Clients see 400 update errors while both leaders are down."]),
]

def pill(m):
    ok = (m.get("rc") == 0 and len(m.get("ranges_committed", [])) == 3
          and m.get("faked_indx_upd", 1) == 0 and m.get("crash_signatures", 1) == 0)
    return '<span class="pill ok">recovered</span>' if ok else '<span class="pill bad">not recovered</span>'

def num(v, unit=""):
    return "—" if v is None else f"{v}{unit}"

rows, cards = [], []
for s in S:
    m = M.get(s["id"])
    if m is None:
        continue
    extra = s.get("steps_override") or s["steps"]
    rec = []
    for k, lab in (("resumes", "resume"), ("redrives", "re-drive"), ("rehomes", "re-home"), ("chain_reforms", "chain re-form")):
        if m.get(k): rec.append(f"{m[k]} {lab}{'s' if m[k] > 1 else ''}")
    recov = ", ".join(rec) if rec else "none needed"
    rows.append(f"""<tr><td><a href="#{s['id']}"><b>{s['tag']}</b></a> <span class="sub">{html.escape(s['title'])}</span></td>
      <td>{html.escape(s['killed'])}</td><td>{pill(m)}</td>
      <td class="n">{len(m['ranges_committed'])} / 3</td><td class="n">{m['faked_indx_upd']}</td>
      <td class="n">{m['update_errors']}</td><td class="n">{m['seconds_below_half']}</td>
      <td class="n">{num(m['migration_s'], ' s')}</td><td>{recov}</td></tr>""")
    steps = "".join(f"<li>{html.escape(x)}</li>" for x in extra)
    cards.append(f"""
<article class="sc" id="{s['id']}">
  <div class="sc-head"><span class="sc-id">{s['tag']}</span><h3>{html.escape(s['title'])}</h3>{pill(m)}</div>
  <dl class="sc-grid">
    <dt>Killed</dt><dd>{html.escape(s['killed'])}{(' at ' + ', '.join(html.escape(k) for k in m['kills'])) if m['kills'] else ''}</dd>
    <dt>When</dt><dd>{html.escape(s['when'])}</dd>
    <dt>Expected</dt><dd>{html.escape(s['expect'])}</dd>
  </dl>
  {'<h4>What happened</h4><ol class="steps">' + steps + '</ol>' if steps else ''}
  <div class="chips">
    <span class="chip"><b>{len(m['ranges_committed'])}/3</b> ranges on sg4</span>
    <span class="chip"><b>{m['faked_indx_upd']}</b> faked INDX_UPD</span>
    <span class="chip"><b>{m['update_errors']}</b> update errors</span>
    <span class="chip"><b>{num(m['tput_pre_kops'])}</b> → <b>{num(m['tput_after_kops'])}</b> Kops/s</span>
    <span class="chip"><b>{m['seconds_below_half']}</b> s below half</span>
    <span class="chip"><b>{num(m['migration_s'], ' s')}</b> migration</span>
    <span class="chip"><b>{m['ae_piggyback_acks']}</b> acks via AppendEntries</span>
  </div>
  <figure>{img('tput_' + s['id'] + '.png', s['tag'] + ' throughput')}
    <figcaption>Both clients' throughput. Shaded: migration (first donor PREP to last TXN_DONE). Dashed red: kill.</figcaption></figure>
  <details><summary>Gantt chart</summary>
    <figure>{img('gantt_' + s['id'] + '.png', s['tag'] + ' Gantt chart')}
      <figcaption>Recipient rows come from redis3's log, so in S1 and S6 they stop when redis3 is killed; donor rows cover the whole run.</figcaption></figure>
  </details>
</article>""")

page = open(os.path.join(here, "failure_report_template.html")).read()
page = page.replace("{{ROWS}}", "\n".join(rows)).replace("{{CARDS}}", "\n".join(cards))
R = json.load(open(os.path.join(FIG, "recovery_times.json")))
KILLED = {s["id"]: s["killed"] for s in S}
rrows = []
for run, r in R.items():
    nl = ", ".join(f"{g} {v:.1f} s" for g, v in r["new_leader_s"].items())
    rrows.append(f"""<tr><td><a href="#{run}"><b>{r['tag']}</b></a> <span class="sub">{html.escape(r['what'])}</span></td>
      <td>{html.escape(KILLED[run])}</td><td class="n">{nl}</td><td class="n">{num(r['first_recovery_s'], ' s')}</td>
      <td class="n"><b>{num(r['all_durable_s'], ' s')}</b></td><td class="n">{r['seconds_below_half']}</td><td class="n">{r['update_errors']}</td></tr>""")
page = page.replace("{{RECOV_FIG}}", img("../recov/compare_full.png", "Before and after the recovery fixes"))
page = page.replace("{{STALL_FIG}}", img("../stall/compare_full.png", "Before and after the follower-pool change"))
page = page.replace("{{ALL70}}", img("tput_all_70s.png", "Throughput, first 70 s, all runs"))
page = page.replace("{{RECOVERY}}", "\n".join(rrows)).replace("{{RECOVERY_FIG}}", img("recovery_times.png", "Recovery time per leader crash"))
out = os.path.join(FIG, "aqraft_failure_campaign.html")
open(out, "w").write(page)
print("wrote", out, f"{os.path.getsize(out)/1e6:.1f} MB")
