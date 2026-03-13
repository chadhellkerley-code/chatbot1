import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from gui.flow_editor.flow_view import FlowBuilderCanvas


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_set_flow_does_not_reemit_programmatic_selection():
    _app()
    canvas = FlowBuilderCanvas()
    selected: list[str] = []
    canvas.stage_selected.connect(selected.append)

    try:
        canvas.set_flow(
            stages=[
                {
                    "id": "stage_1",
                    "action_type": "mensaje",
                    "transitions": {},
                    "followups": [],
                    "post_objection": {"enabled": False},
                }
            ],
            pack_options=[],
            entry_stage_id="stage_1",
            positions={"stage_1": (120.0, 120.0)},
            viewport={"zoom": 1.0, "pan_x": 0.0, "pan_y": 0.0},
            selected_stage_id="stage_1",
        )
    finally:
        canvas.close()

    assert selected == []
