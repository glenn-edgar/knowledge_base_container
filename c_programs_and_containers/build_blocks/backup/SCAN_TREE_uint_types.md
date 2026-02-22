# Scan Tree Type Definitions

---

## 1. Primitive Types

Scan Tree supports the following primitive types:

### Boolean

- `bool`

### Unsigned Integers

- `uint8_t`
- `uint16_t`
- `uint32_t`
- `uint64_t`

### Signed Integers

- `int8_t`
- `int16_t`
- `int32_t`
- `int64_t`

### Floating Point

- `float`
- `double`

---

## 2. Buffer Type Constraint

A Scan Tree buffer consists of exactly one type. All elements within a single buffer share the same primitive type. There are no mixed-type buffers.

---

## 3. Automatic Type Conversion

Scan Tree operators automatically convert types to the target format. When a virtual function's inputs come from buffers of differing types, the operator handles the conversion transparently. The target format is determined by the operator, not by the source buffers.