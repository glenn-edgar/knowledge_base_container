def WindowLiftController(ct, kb_name="kb.window_lift"):
    """
    Full automotive power-window controller with anti-pinch (EU/UN-R 21 compliant)
    Root node is a LibraryNode with repeat = true → runs forever once started
    """
    ct.start_test(test_name=kb_name)               # Root LibraryNode

    # ────────────────────────────────────────────────────────────────────────
    # Main state machine – only one child column is ever enabled at a time
    # ────────────────────────────────────────────────────────────────────────
    main_sm = ct.define_state_machine(
        column_name="main_sm",
        sm_name="WINDOW_MAIN_SM",
        state_names=[
            "IdleMonitoring", "DirectionDecision", "ManualUpSequence",
            "ManualDownSequence", "ExpressUpSequence", "ExpressDownSequence",
            "EmergencyReverse"
        ],
        initial_state="IdleMonitoring",
        auto_start=True
    )

    # ========================================================================
    # IdleMonitoring – wait for any user input
    # ========================================================================
    idle = ct.define_state("IdleMonitoring")
    ct.asm_log_message("WindowLift: Idle monitoring")
    ct.asm_wait_for_event(
        event_id="INPUT_ANY",
        event_count=1,
        reset_flag=True,
        timeout=0,                                 # no timeout – wait forever
        generate_events=["SwitchUp", "SwitchDown", "RemoteClose",
                         "ExpressUp", "ExpressDown"]
    )
    ct.change_state(main_sm, "DirectionDecision")
    ct.end_column(idle)

    # ========================================================================
    # DirectionDecision – priority fallback (express beats manual beats remote)
    # ========================================================================
    decision = ct.define_state("DirectionDecision")
    ct.asm_log_message("WindowLift: Deciding direction")

    # PriorityChoice implemented as a Fallback composite (first success wins)
    priority = ct.define_fallback("priority_choice")
    
    ct.asm_if_event(event_id="ExpressUp")          ; ct.change_state(main_sm, "ExpressUpSequence")   ; ct.asm_success()
    ct.asm_if_event(event_id="ExpressDown")        ; ct.change_state(main_sm, "ExpressDownSequence") ; ct.asm_success()
    ct.asm_if_event(event_id="SwitchUp")           ; ct.change_state(main_sm, "ManualUpSequence")    ; ct.asm_success()
    ct.asm_if_event(event_id="SwitchDown")         ; ct.change_state(main_sm, "ManualDownSequence")  ; ct.asm_success()
    ct.asm_if_event(event_id="RemoteClose")        ; ct.change_state(main_sm, "ExpressUpSequence")   ; ct.asm_success()
    
    ct.end_column(priority)   # fallback never fails → stays in DirectionDecision forever
    ct.end_column(decision)

    # ========================================================================
    # ManualUpSequence
    # ========================================================================
    manual_up = ct.define_state("ManualUpSequence")
    ct.asm_sequence("manual_up_seq", skip_on="AntiPinchTriggered || OverCurrent || HallError")

    ct.asm_call("SoftStartRamp_Up",   {"duration_ms": 200})
    ct.asm_call("ConstantSpeed_Up")
    ct.asm_wait_condition("InSoftStopZone",   reset_flag=False)
    ct.asm_call("SoftStopRamp_Up")
    ct.asm_wait_any(
        conditions=["SwitchReleased", "Position >= MaxPosition"],
        reset_flag=True
    )
    ct.asm_call("MotorBrake")
    ct.change_state(main_sm, "IdleMonitoring")
    ct.end_column(manual_up)

    # ========================================================================
    # ManualDownSequence (simpler – no anti-pinch)
    # ========================================================================
    manual_down = ct.define_state("ManualDownSequence")
    ct.asm_sequence("manual_down_seq")
    ct.asm_call("SoftStartRamp_Down", {"duration_ms": 180})
    ct.asm_call("ConstantSpeed_Down")
    ct.asm_wait_condition("SwitchReleased || Position <= MinPosition", reset_flag=True)
    ct.asm_call("MotorBrake")
    ct.change_state(main_sm, "IdleMonitoring")
    ct.end_column(manual_down)

    # ========================================================================
    # ExpressUpSequence – full featured with anti-pinch and normalization
    # ========================================================================
    express_up = ct.define_state("ExpressUpSequence")
    ct.asm_sequence("express_up_seq")

    # ---- Optional normalization ------------------------------------------------
    ct.asm_if_not("NormalizationValid && CyclesSinceNorm > 5")
    ct.asm_call("RunNormalization")                     # full cycles + build ripple map
    ct.asm_fi()

    ct.asm_call("SoftStartRamp_Up",   {"duration_ms": 200})
    ct.asm_call("ConstantSpeed_Up_WithAntiPinch")

    # Enable anti-pinch only after entering pinch protection zone
    ct.asm_wait_condition("Position > PinchZoneStart", reset_flag=False)
    ct.asm_enable_node("AntiPinchDetector_Up")

    ct.asm_wait_condition("InSoftStopZone", reset_flag=False)
    ct.asm_call("SoftStopRamp_Up")

    ct.asm_wait_condition("Position >= MaxPosition - SealZone", reset_flag=False)
    ct.asm_call("MotorBrake_And_HoldPWM", {"hold_duty": 8})

    ct.asm_wait_any(
        conditions=["PositionFullyClosed", "AntiPinchTriggered"],
        reset_flag=True
    )
    ct.asm_if("AntiPinchTriggered")
        ct.change_state(main_sm, "EmergencyReverse")
    ct.asm_else()
        ct.change_state(main_sm, "IdleMonitoring")
    ct.asm_fi()
    ct.end_column(express_up)

    # ========================================================================
    # ExpressDownSequence – no anti-pinch required by regulation
    # ========================================================================
    express_down = ct.define_state("ExpressDownSequence")
    ct.asm_sequence("express_down_seq")
    ct.asm_call("SoftStartRamp_Down", {"duration_ms": 180})
    ct.asm_call("ConstantSpeed_Down")
    ct.asm_wait_condition("Position <= MinPosition", reset_flag=True)
    ct.asm_call("MotorBrake")
    ct.change_state(main_sm, "IdleMonitoring")
    ct.end_column(express_down)

    # ========================================================================
    # EmergencyReverse – highest priority override (parallel + invariant)
    # ========================================================================
    emergency = ct.define_state("EmergencyReverse")
    ct.asm_sequence("emergency_reverse", invariant="AntiPinchTriggered || OverCurrent || ThermalShutdown")

    ct.asm_call("Immediate_MotorStop_And_Brake")
    ct.asm_call("ReverseDirection")
    ct.asm_call("SoftStartRamp_Down", {"duration_ms": 80})
    ct.asm_wait_any(
        conditions=["TravelledDistance >= 150mm", "ReverseTime >= 400ms"],
        reset_flag=False
    )
    ct.asm_call("MotorBrake")
    ct.asm_clear_flag("AntiPinchTriggered")
    ct.change_state(main_sm, "IdleMonitoring")
    ct.end_column(emergency)

    # ========================================================================
    # FaultHandler – always-active parallel supervisor (runs in parallel with main SM)
    # ========================================================================
    fault = ct.define_parallel("FaultHandler", required_success=0, auto_start=True)  # always active

    ct.asm_verify("!OverTemperature",       error_fn="TRIGGER_EMERGENCY", error_data={"reason":"thermal"})
    ct.asm_verify("!HallSensorError",       error_fn="TRIGGER_EMERGENCY", error_data={"reason":"hall"})
    ct.asm_verify("!SupplyUnderVoltage",    error_fn="TRIGGER_EMERGENCY", error_data={"reason":"uvlo"})
    ct.asm_verify("!CurrentLimitExceeded",  error_fn="TRIGGER_EMERGENCY", error_data={"reason":"oc"})

    # Helper that jumps to EmergencyReverse from anywhere
    ct.define_leaf("TRIGGER_EMERGENCY", fn="TriggerEmergencyReverse", auto_start=False)

    ct.end_column(fault)

    # ========================================================================
    # Anti-pinch detector – parallel child used only when enabled
    # ========================================================================
    antipinch = ct.define_parallel("AntiPinchDetector_Up", auto_start=False)
    ct.asm_verify("RipplePeriodOK", error_fn="SET_ANTIPINCH_FLAG")
    ct.asm_verify("DeltaPeriod <= Threshold(Position) || ConfirmationCount < 3",
                  error_fn="SET_ANTIPINCH_FLAG")
    ct.define_leaf("SET_ANTIPINCH_FLAG", fn="SetAntiPinchTriggered", auto_start=False)
    ct.end_column(antipinch)

    # ────────────────────────────────────────────────────────────────────────
    ct.end_state_machine(main_sm)
    ct.end_test()