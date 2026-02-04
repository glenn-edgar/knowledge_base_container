local mod = start_module("demo")

RECORD("state")
    FIELD("counter", "uint32")
    FIELD("mode", "uint32")
END_RECORD()

start_tree("main")
use_record("state")

local root = m_call("SE_SEQUENCE")

    local n = m_call("SE_EVENT_DISPATCH")
        json({
            handlers = {
                on_press = main("SE_SEQUENCE",
                    oneshot("SE_LOG", "pressed"),
                    oneshot("SE_INC_FIELD", field("counter")),
                    main("SE_IF_THEN_ELSE",
                        pred("SE_FIELD_GT", field("counter"), 10),
                        oneshot("SE_LOG", "threshold exceeded"),
                        main("SE_RETURN_CONTINUE")
                    )
                ),
                
                on_timeout = main("SE_SEQUENCE",
                    oneshot("SE_SET_FIELD", field("mode"), 0),
                    main("SE_RETURN_HALT")
                ),
                
                guard = pred_c("SE_PRED_AND",
                    pred("SE_FIELD_NE", field("mode"), 255),
                    pred("SE_TRUE")
                )
            },
            
            default = func("SE_RETURN_CONTINUE"),
            
            config = {
                timeout_ms = 5000,
                retry_count = 3
            }
        })
    end_call(n)

end_call(root)

end_tree("main")

return end_module(mod)

