"""Generate ``examples/donki_quickstart.ipynb`` from this script.

Run from the repo root:

    python examples/build_donki_quickstart.py

We generate the notebook programmatically so it stays in sync with the
adapter API. The notebook is not pre-executed; CI executes it via
``jupyter nbconvert --execute`` in a follow-up cycle.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

NOTEBOOK_PATH = Path(__file__).parent / "donki_quickstart.ipynb"


def build_notebook() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    nb["metadata"] = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.12",
        },
    }
    cells: list[nbf.NotebookNode] = []

    cells.append(
        nbf.v4.new_markdown_cell(
            "# DONKI Quickstart — the May 2024 Gannon storm\n"
            "\n"
            "This notebook demonstrates the `DonkiAdapter` against the\n"
            "May 2024 Gannon G5 storm window (2024-05-08 → 2024-05-15).\n"
            "We fetch CMEs and solar flares, then walk DONKI's *intelligent\n"
            "linkages* to show how a geomagnetic storm traces back to its\n"
            "originating coronal mass ejections.\n"
            "\n"
            "The Gannon storm is the strongest event in HELIOS's "
            "pre-registered hold-out set (proposal §3 Table 3-1) and the\n"
            "marquee example for the precision-ag GNSS slice (§1.3).\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "from datetime import UTC, datetime\n"
            "\n"
            "from helios_connectors import DonkiAdapter\n"
            "from helios_connectors.adapters.donki import DONKI_KAUAI_BASE_URL\n"
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            "## 1. Fetch CMEs and flares for the Gannon window\n"
            "\n"
            "We point the adapter at CCMC's `kauai` endpoint to avoid the\n"
            "`api.nasa.gov` DEMO_KEY rate cap. Set `NASA_API_KEY` in your\n"
            "environment to use the default `api.nasa.gov` route instead.\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "start = datetime(2024, 5, 8, tzinfo=UTC)\n"
            "end = datetime(2024, 5, 15, tzinfo=UTC)\n"
            "\n"
            "async with DonkiAdapter(base_url=DONKI_KAUAI_BASE_URL) as donki:\n"
            "    cmes = [r async for r in donki.fetch_cme(start=start, end=end)]\n"
            "    flares = [r async for r in donki.fetch_flr(start=start, end=end)]\n"
            "    gsts = [r async for r in donki.fetch_gst(start=start, end=end)]\n"
            "\n"
            "print(f'CMEs:   {len(cmes)}')\n"
            "print(f'Flares: {len(flares)}')\n"
            "print(f'GSTs:   {len(gsts)}')\n"
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            "## 2. Inspect a normalized record\n"
            "\n"
            "Each record carries a science payload (`value`) plus a\n"
            "`ProvenanceRecord` describing where the value came from\n"
            "and what upstream events it depends on.\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "cme = cmes[0]\n"
            "print('record_type:', cme.record_type)\n"
            "print('event_time:', cme.event_time)\n"
            "print('provenance.id:', cme.provenance.id)\n"
            "print('provenance.model_id:', cme.provenance.model_id)\n"
            "print('provenance.lineage:', cme.provenance.lineage)\n"
            "print('value keys:', sorted(cme.value.keys())[:8])\n"
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            "## 3. Trace the Gannon G5 storm back to its source CMEs\n"
            "\n"
            "DONKI's geomagnetic-storm records carry `linkedEvents` pointing\n"
            "back at the originating coronal mass ejections. The HELIOS\n"
            "adapter surfaces these as `provenance.lineage`. This is the\n"
            "key affordance for downstream fusion: every output traces to\n"
            "every contributing upstream event.\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "gannon = next(\n"
            "    g for g in gsts if g.provenance.id.startswith('2024-05-10')\n"
            ")\n"
            "print(f'Gannon GST id: {gannon.provenance.id}')\n"
            "print(f'Event time: {gannon.event_time}')\n"
            "print()\n"
            "print(f'Lineage ({len(gannon.provenance.lineage)} upstream events):')\n"
            "for upstream in gannon.provenance.lineage:\n"
            "    print(f'  - {upstream}')\n"
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            "## 4. Plot a timeline of events\n"
            "\n"
            "A simple scatterplot of event_time per record_type makes the\n"
            "cadence of the storm visible at a glance: a burst of flares,\n"
            "the CMEs that propagated outward, and the resulting GST.\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "import matplotlib.pyplot as plt\n"
            "\n"
            "fig, ax = plt.subplots(figsize=(10, 4))\n"
            "for label, recs, color in [\n"
            "    ('Flares', flares, 'tab:red'),\n"
            "    ('CMEs', cmes, 'tab:blue'),\n"
            "    ('GSTs', gsts, 'tab:purple'),\n"
            "]:\n"
            "    times = [r.event_time for r in recs]\n"
            "    ax.scatter(times, [label] * len(times), color=color, s=40, alpha=0.7)\n"
            "ax.set_title('DONKI events: May 2024 Gannon storm window')\n"
            "ax.set_xlabel('UTC')\n"
            "ax.grid(alpha=0.3)\n"
            "fig.autofmt_xdate()\n"
            "plt.tight_layout()\n"
            "plt.show()\n"
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            "## What's next\n"
            "\n"
            "- Swap `DonkiAdapter` for `SwpcAdapter` (forthcoming) to ingest\n"
            "  the operational `Kp` series alongside DONKI events.\n"
            "- Drop the records into `helios-fusion-engine` to feed a BMA\n"
            "  fusion pipeline (proposal §2 Obj. 2).\n"
            "- Each `ProvenanceRecord` is forward-compatible with the\n"
            "  `helios-provenance-spec` v0.1 schema; once that ships, this\n"
            "  notebook will validate every record against the schema.\n"
        )
    )

    nb["cells"] = cells
    return nb


def main() -> None:
    nb = build_notebook()
    with NOTEBOOK_PATH.open("w") as f:
        nbf.write(nb, f)
    print(f"wrote {NOTEBOOK_PATH}")
    # Format the notebook so subsequent `ruff format --check` is a no-op.
    try:
        import subprocess

        subprocess.run(
            ["ruff", "format", str(NOTEBOOK_PATH)],
            check=False,
            capture_output=True,
        )
    except FileNotFoundError:  # pragma: no cover
        pass


if __name__ == "__main__":
    main()
