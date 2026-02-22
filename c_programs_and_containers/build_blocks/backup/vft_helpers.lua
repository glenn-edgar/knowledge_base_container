--[[
    Scan Tree VFT Helper File - Standard Virtual Function Templates (LuaJIT)

    Each VFT is a construction-time function that:
      - Validates parameter count, types, and ranges
      - Returns a record for the JSON intermediate file

    VFT calling convention:
      - First parameter is always the output: one bit in a boolean buffer
      - Subsequent parameters are inputs
      - Parameter format: buffer_name:start_position-count
--]]

local M = {}

-- ------------------------------------------------------------------
-- VFT_and: Boolean AND across all input bits
-- ------------------------------------------------------------------
function M.VFT_and(dsl, output_param, input_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_and: output must be exactly 1 bit, got %d", out_count))
    end

    local in_buf, in_start, in_count = dsl:validate_param(
        input_param, {require_bool = true})
    if in_count == 0 then
        error("VFT_and: input count must be >= 1")
    end

    return {
        vft_name = "VFT_and",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                buffer = in_buf.path,
                start = in_start,
                count = in_count,
            },
        },
    }
end

-- ------------------------------------------------------------------
-- VFT_or: Boolean OR across all input bits
-- ------------------------------------------------------------------
function M.VFT_or(dsl, output_param, input_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_or: output must be exactly 1 bit, got %d", out_count))
    end

    local in_buf, in_start, in_count = dsl:validate_param(
        input_param, {require_bool = true})
    if in_count == 0 then
        error("VFT_or: input count must be >= 1")
    end

    return {
        vft_name = "VFT_or",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                buffer = in_buf.path,
                start = in_start,
                count = in_count,
            },
        },
    }
end

-- ------------------------------------------------------------------
-- VFT_not: Boolean NOT of a single input bit
-- ------------------------------------------------------------------
function M.VFT_not(dsl, output_param, input_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_not: output must be exactly 1 bit, got %d", out_count))
    end

    local in_buf, in_start, in_count = dsl:validate_param(
        input_param, {require_bool = true})
    if in_count ~= 1 then
        error(string.format("VFT_not: input must be exactly 1 bit, got %d", in_count))
    end

    return {
        vft_name = "VFT_not",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                buffer = in_buf.path,
                start = in_start,
                count = 1,
            },
        },
    }
end

-- ------------------------------------------------------------------
-- VFT_k_of_n: Voting gate (at least K of N inputs true)
-- ------------------------------------------------------------------
function M.VFT_k_of_n(dsl, output_param, threshold_param, input_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_k_of_n: output must be exactly 1 bit, got %d", out_count))
    end

    local thresh_buf, thresh_start, thresh_count = dsl:validate_param(threshold_param)
    if thresh_count ~= 1 then
        error(string.format("VFT_k_of_n: threshold must be exactly 1 element, got %d", thresh_count))
    end

    local in_buf, in_start, in_count = dsl:validate_param(
        input_param, {require_bool = true})
    if in_count < 2 then
        error(string.format("VFT_k_of_n: input count must be >= 2, got %d", in_count))
    end

    return {
        vft_name = "VFT_k_of_n",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                role = "threshold",
                buffer = thresh_buf.path,
                start = thresh_start,
                count = 1,
            },
            {
                role = "bits",
                buffer = in_buf.path,
                start = in_start,
                count = in_count,
            },
        },
    }
end

-- ------------------------------------------------------------------
-- VFT_gt: Comparison (output true if input_a > input_b)
-- ------------------------------------------------------------------
function M.VFT_gt(dsl, output_param, input_a_param, input_b_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_gt: output must be exactly 1 bit, got %d", out_count))
    end

    local in_a_buf, in_a_start, in_a_count = dsl:validate_param(input_a_param)
    if in_a_count ~= 1 then
        error(string.format("VFT_gt: input_a must be exactly 1 element, got %d", in_a_count))
    end

    local in_b_buf, in_b_start, in_b_count = dsl:validate_param(input_b_param)
    if in_b_count ~= 1 then
        error(string.format("VFT_gt: input_b must be exactly 1 element, got %d", in_b_count))
    end

    return {
        vft_name = "VFT_gt",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                role = "a",
                buffer = in_a_buf.path,
                start = in_a_start,
                count = 1,
            },
            {
                role = "b",
                buffer = in_b_buf.path,
                start = in_b_start,
                count = 1,
            },
        },
    }
end

-- ------------------------------------------------------------------
-- VFT_latch: Latching fault gate
-- ------------------------------------------------------------------
function M.VFT_latch(dsl, output_param, set_param, clear_param)
    local out_buf, out_start, out_count = dsl:validate_param(
        output_param, {require_bool = true})
    if out_count ~= 1 then
        error(string.format("VFT_latch: output must be exactly 1 bit, got %d", out_count))
    end

    local set_buf, set_start, set_count = dsl:validate_param(
        set_param, {require_bool = true})
    if set_count ~= 1 then
        error(string.format("VFT_latch: set input must be exactly 1 bit, got %d", set_count))
    end

    local clr_buf, clr_start, clr_count = dsl:validate_param(
        clear_param, {require_bool = true})
    if clr_count ~= 1 then
        error(string.format("VFT_latch: clear input must be exactly 1 bit, got %d", clr_count))
    end

    return {
        vft_name = "VFT_latch",
        output = {
            buffer = out_buf.path,
            start = out_start,
            count = 1,
        },
        inputs = {
            {
                role = "set",
                buffer = set_buf.path,
                start = set_start,
                count = 1,
            },
            {
                role = "clear",
                buffer = clr_buf.path,
                start = clr_start,
                count = 1,
            },
        },
    }
end

return M

