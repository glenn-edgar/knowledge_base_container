es — all the measurement methods work while the DriveBase is actively driving. That's exactly what they're for.
drive_base.state() is the one-call snapshot that gives you everything at once: (distance_mm, speed_mm_s, angle_deg, turn_rate_deg_s). You can call it every loop iteration while drive() is running:
pythondb.use_gyro(True)
db.drive(200, 0)  # start driving forward

while True:
    dist, speed, angle, turn_rate = db.state()
    print(dist, speed, angle, turn_rate)
    
    if dist > 500:  # stop after 500 mm
        db.stop()
        break
    wait(10)
The individual methods also work during motion:
MethodAvailable During DriveNotesdrive_base.distance()YesAccumulated mm since last reset()drive_base.angle()YesAccumulated heading degrees since last reset()drive_base.state()YesAll four values in one call — most efficientdrive_base.stalled()YesReal-time stall detectiondrive_base.done()YesFor wait=False commandshub.imu.heading()YesAlways available, independent of DriveBasehub.imu.acceleration()YesAlways availablehub.imu.angular_velocity()YesAlways available
What you cannot read during active DriveBase:
MethodAvailable During DriveWhymotor.angle()NoMotor is owned by DriveBasemotor.speed()NoMotor is owned by DriveBasemotor.load()NoMotor is owned by DriveBase
This is the motor lock constraint — the individual Motor objects are locked out while the DriveBase is active. You get the fused odometry through state() instead of raw per-wheel encoder values.
The typical pattern for a sensor-driven control loop is:
pythondb.use_gyro(True)
db.drive(150, 0)

while True:
    dist, speed, heading, turn_rate = db.state()
    
    # Use distance for position control
    if dist >= target_distance:
        db.stop()
        break
    
    # Use heading for correction
    heading_error = desired_heading - heading
    db.drive(150, heading_error * 1.5)  # proportional heading correction
    
    wait(10)
So the answer to your question is: distance, heading, speed, and turn rate are all live during drive() mode. The only thing you lose is individual wheel encoder access — you see the robot-level kinematics, not the per-motor raw data.how can s curve principles be incorporated into the motor drive operation?12:20 PMExamined S-curve implementation gaps and custom coding requirementsExamined S-curve implementation gaps and custom coding requirementsPybricks doesn't have native S-curve (jerk-limited) motion profiles — its built-in run_target(), straight(), turn() etc. all use trapezoidal velocity profiles with constant acceleration. The acceleration snaps on instantly, which is exactly the problem S-curves solve.
You have two approaches: implement S-curve velocity generation in your control loop feeding drive(), or implement S-curve position generation feeding track_target().
What S-Curve Does
A trapezoidal profile has three phases: accelerate at constant rate, cruise, decelerate at constant rate. The jerk (rate of change of acceleration) is infinite at the transitions — the acceleration snaps from 0 to max instantly. This causes wheel slip, mechanical shock, and oscillation.
An S-curve adds jerk limiting so there are seven phases: increasing acceleration, constant acceleration, decreasing acceleration, cruise, increasing deceleration, constant deceleration, decreasing deceleration. The acceleration ramps smoothly.
Approach 1: S-Curve Velocity Profile with drive()
Feed a time-varying speed to drive() every loop iteration. You compute the S-curve profile offline or incrementally:
pythonfrom pybricks.pupdevices import Motor
from pybricks.parameters import Port, Direction
from pybricks.robotics import DriveBase
from pybricks.tools import wait, StopWatch
from umath import sqrt

left = Motor(Port.A, Direction.COUNTERCLOCKWISE)
right = Motor(Port.B)
db = DriveBase(left, right, 56, 112)
db.use_gyro(True)

def s_curve_drive(target_dist, max_speed, max_accel, max_jerk):
    """
    Drive straight using an S-curve velocity profile.
    
    target_dist: mm to travel
    max_speed:   mm/s cruise speed
    max_accel:   mm/s^2 max acceleration
    max_jerk:    mm/s^3 rate of change of acceleration
    """
    db.reset()
    watch = StopWatch()
    
    # Current state
    accel = 0.0
    speed = 0.0
    dt = 0.010  # 10 ms loop period
    
    # Phase: 0=accel ramp up, 1=const accel, 2=accel ramp down,
    #        3=cruise, 4=decel ramp up, 5=const decel, 6=decel ramp down
    
    while True:
        dist = db.distance()
        remaining = target_dist - dist
        
        # Compute stopping distance from current speed with S-curve decel
        # Approximate: d_stop ≈ v^2 / (2*a) but with jerk correction
        if max_accel > 0:
            d_stop = (speed * speed) / (2.0 * max_accel) + \
                     (speed * max_accel) / (2.0 * max_jerk)
        else:
            d_stop = 0
        
        if remaining <= d_stop:
            # Deceleration phase — ramp acceleration negative
            if accel > -max_accel:
                accel = max(accel - max_jerk * dt, -max_accel)
            # Near zero speed, ramp acceleration back toward zero
            if speed < max_accel * max_accel / max_jerk:
                accel = min(accel + max_jerk * dt, 0)
        else:
            # Acceleration / cruise phase
            if speed < max_speed:
                # Ramp up acceleration
                if accel < max_accel:
                    accel = min(accel + max_jerk * dt, max_accel)
                # Approaching cruise — ramp down acceleration
                speed_to_cruise = max_speed - speed
                if speed_to_cruise < accel * accel / (2.0 * max_jerk):
                    accel = max(accel - max_jerk * dt, 0)
            else:
                # At cruise — zero acceleration
                if accel > 0:
                    accel = max(accel - max_jerk * dt, 0)
        
        # Integrate
        speed = max(speed + accel * dt, 0)
        speed = min(speed, max_speed)
        
        # Command the DriveBase
        db.drive(speed, 0)
        
        # Termination
        if remaining <= 2 and speed < 5:
            db.stop()
            break
        
        wait(10)
Approach 2: S-Curve Position Profile with track_target()
For a single motor (like an arm), track_target() skips the internal acceleration planner and jumps to whatever angle you command. So you pre-compute the S-curve position trajectory and feed it sample by sample:
pythonfrom pybricks.pupdevices import Motor
from pybricks.parameters import Port
from pybricks.tools import wait, StopWatch

arm = Motor(Port.E)

def s_curve_profile(target_angle, max_speed, max_accel, max_jerk, dt=0.010):
    """
    Generator that yields position samples for an S-curve profile.
    All units in degrees, deg/s, deg/s^2, deg/s^3.
    """
    pos = 0.0
    vel = 0.0
    acc = 0.0
    direction = 1.0 if target_angle >= 0 else -1.0
    target = abs(target_angle)
    
    while True:
        remaining = target - pos
        
        # Stopping distance estimate
        if max_accel > 0:
            d_stop = (vel * vel) / (2.0 * max_accel) + \
                     (vel * max_accel) / (2.0 * max_jerk)
        else:
            d_stop = 0
        
        if remaining <= d_stop:
            # Deceleration S-curve
            if acc > -max_accel:
                acc = max(acc - max_jerk * dt, -max_accel)
            if vel < max_accel * max_accel / max_jerk:
                acc = min(acc + max_jerk * dt, 0)
        elif vel < max_speed:
            # Acceleration S-curve
            speed_to_cruise = max_speed - vel
            if speed_to_cruise > acc * acc / (2.0 * max_jerk):
                acc = min(acc + max_jerk * dt, max_accel)
            else:
                acc = max(acc - max_jerk * dt, 0)
        else:
            # Cruise
            acc = max(acc - max_jerk * dt, 0)
        
        vel = max(vel + acc * dt, 0)
        vel = min(vel, max_speed)
        pos += vel * dt
        
        if pos >= target or (remaining <= 1 and vel < 1):
            yield direction * target
            break
        
        yield direction * pos

def s_curve_move(motor, target_angle, max_speed, max_accel, max_jerk):
    """Execute an S-curve move on a single motor."""
    start = motor.angle()
    
    for pos in s_curve_profile(target_angle, max_speed, max_accel, max_jerk):
        motor.track_target(start + pos)
        wait(10)
    
    motor.hold()
Approach 3: Asymmetric Acceleration as a Partial S-Curve
If you don't need a full 7-phase S-curve but just want to soften the transitions, Pybricks settings() already supports separate acceleration and deceleration values:
python# Separate accel/decel — not a true S-curve but reduces the worst jerk
db.settings(
    straight_speed=300,
    straight_acceleration=(200, 400),  # gentle accel, firmer decel
    turn_rate=150,
    turn_acceleration=(100, 200)
)
And on individual motors:
pythonmotor.control.limits(
    speed=800,          # deg/s
    acceleration=500,   # deg/s^2 — lower = softer ramp
    torque=300          # mNm — limits contact force
)
This doesn't shape the jerk, but lowering the acceleration value has a similar practical effect of reducing wheel slip and mechanical shock.
Practical Considerations
Loop rate matters. The SPIKE hub runs MicroPython at roughly 10 ms loop periods with wait(10). That's 100 Hz — adequate for S-curve profiles on a LEGO robot where mechanical time constants are long. The jerk parameter needs to be tuned relative to this loop rate.
Tuning parameters for SPIKE robots:
ParameterTypical RangeNotesmax_speed100–400 mm/sDriveBase; depends on wheel size and gear ratiomax_accel200–800 mm/s²Lower = smoother, higher = more responsivemax_jerk500–3000 mm/s³The new parameter S-curve adds. Lower = smoother transitions. Start at max_accel / 0.2 (reach max accel in 200 ms)
drive() vs track_target() tradeoffs:
MethodUse CaseAdvantageLimitationdrive(speed, turn_rate)DriveBase straight/arc S-curvesWorks with full kinematics, gyro fusion activeYou command velocity, not position — need your own position tracking for distance targetstrack_target(angle)Single motor position S-curvesDirect position control, simple to implementNo smooth acceleration planner — relies entirely on your profile generator. DriveBase doesn't have track_target().dc(duty)Lowest-level S-curveFull control over outputNo PID, no speed/position feedback — you'd implement everything yourself
Why Pybricks doesn't build this in: The internal motion controller uses a trapezoidal profile because it's computationally cheap and the PID controller handles residual vibration. On a LEGO robot with compliant plastic gears and rubber tires, the mechanical system itself acts as a low-pass filter that softens jerk naturally. S-curves become important when you have stiff drivetrains, heavy payloads, or need to minimize wheel slip on low-traction surfaces — which is exactly your competition/precision use case.how is the pid configured and how is it related to the robot drive system.12:23 PMSynthesized PID mechanics and robot integration conceptsSynthesized PID mechanics and robot integration conceptsPybricks uses PID controllers at every level — individual motors, and then two more layered on top for the DriveBase. Here's how the whole architecture works.
The Control Hierarchy
There are three distinct PID controller instances in a typical DriveBase setup:
DriveBase
├── distance_control  ─── PID → controls how far the robot travels (mm)
├── heading_control   ─── PID → controls the robot's heading angle (deg)
│
├── left Motor
│   └── motor.control   ── PID → controls this motor's angle/speed (deg)
└── right Motor
    └── motor.control   ── PID → controls this motor's angle/speed (deg)
The Motor class uses PID control to accurately track your commanded target angles. Similarly, the DriveBase class uses two of such controllers: one to control the heading and one to control the traveled distance. Pybricks
The DriveBase-level controllers compute a desired speed for each motor based on the distance and heading errors. Those desired speeds become the setpoints for the individual motor PID controllers, which then compute the actual duty cycle (voltage) sent to the motor H-bridge.
The Control Class API
Every PID controller in the system — whether on a Motor or on a DriveBase — is an instance of the same Control class, accessed via the .control attribute (motors) or .distance_control / .heading_control (DriveBase).
You can only change the settings while the controller is stopped. For example, you can set the settings at the beginning of your program. Alternatively, first call stop() to make your Motor or DriveBase stop, and then change the settings. Pybricks
control.pid(kp, ki, kd, integral_deadzone, integral_rate)
The core PID gains. Call with no arguments to read current values.
ParameterUnitsDescriptionkp PybricksµNm/deg PybricksProportional position control constant. It is the feedback torque per degree of error. Pybricks This is the primary stiffness term — how hard the controller pushes to correct position error.ki PybricksµNm/(deg s) PybricksIntegral position control constant. It is the feedback torque per accumulated degree of error. Pybricks Eliminates steady-state error — the residual offset that P alone can't close.kd PybricksµNm/(deg/s) PybricksDerivative position (or proportional speed) control constant. It is the feedback torque per unit of speed. Pybricks Provides damping — resists rapid changes and reduces overshoot.integral_deadzone Pybricksdeg or mm PybricksZone around the target where the error integral does not accumulate errors. Pybricks Prevents integral windup when the motor is close to target.integral_rate Pybricksdeg/s or mm/s PybricksMaximum rate at which the error integral is allowed to grow. Limits how fast the I term ramps up.
For the v3.0 and earlier API, there was also a feed_forward parameter (percentage) that added a signal in the direction of the speed reference — essentially an open-loop term to help the PID during acceleration.
Example default values for a SPIKE angular motor:
pythonmotor = Motor(Port.A)
print(motor.control.pid())
# Typical: (kp=42484, ki=21242, kd=5310, integral_deadzone=8, integral_rate=15)
control.limits(speed, acceleration, torque)
Caps the maximum actuation. Call with no arguments to read current values.
ParameterUnitsDescriptionspeeddeg/s or mm/sMaximum speed the controller will command. All speed commands are capped to this.accelerationdeg/s² or mm/s²Maximum acceleration. Can be a tuple (accel, decel) for asymmetric profiles.torquemNmMaximum feedback torque during control. Limits the physical force the motor applies.
pythonmotor.control.limits(800, 500, 300)
# Max 800 deg/s, 500 deg/s² accel, 300 mNm torque
control.target_tolerances(speed, position)
Defines when done() returns True — how close to the target counts as "finished."
ParameterUnitsDescriptionspeeddeg/s or mm/sAllowed deviation from zero speed before motion is considered complete.positiondeg or mmAllowed deviation from the target angle/distance before motion is considered complete.
Tighter tolerances mean the motor takes longer to settle. Looser tolerances mean done() fires sooner but the motor may still be oscillating slightly.
control.stall_tolerances(speed, time)
Defines when stalled() returns True.
ParameterUnitsDescriptionspeed Pybricksdeg/s or mm/s PybricksIf the controller cannot reach this speed for some time even with maximum actuation, it is stalled. Pybrickstime Pybricksms PybricksHow long the controller has to be below this minimum speed before we say it is stalled. Pybricks
control.scale
Read-only. Scaling factor between the controlled integer variable and the physical output. For example, for a single motor this is the number of encoder pulses per degree of rotation. Pybricks Automatically set by the gears parameter when constructing the Motor.
model.state()
Gets the estimated angle, speed, current, and stall state of the motor, using a simulation model that mimics the real motor. These estimates are updated faster than the real measurements, which can be useful when building your own PID controllers. Pybricks
Returns (angle_deg, speed_deg_s, current_mA, stalled_bool).
How It Applies to DriveBase
The DriveBase has two independent PID controllers that sit above the motor-level controllers:
pythonfrom pybricks.pupdevices import Motor
from pybricks.parameters import Port, Direction
from pybricks.robotics import DriveBase

left = Motor(Port.A, Direction.COUNTERCLOCKWISE)
right = Motor(Port.B)
db = DriveBase(left, right, 56, 112)

# Read DriveBase PID settings
print("Distance PID:", db.distance_control.pid())
print("Distance limits:", db.distance_control.limits())
print("Heading PID:", db.heading_control.pid())
print("Heading limits:", db.heading_control.limits())

# Read individual motor PID settings
print("Left motor PID:", left.control.pid())
print("Right motor PID:", right.control.pid())
```

The heading_control attribute has the same functionality, but the settings apply to every degree of rotation of the whole drive base (viewed from the top) instead of degrees turned by one motor. 

## The Signal Flow

Here's what happens when you call `db.straight(500)`:
```
1. Motion Planner
   └── Generates a trapezoidal velocity profile for 500 mm
       (accelerate → cruise → decelerate)

2. distance_control PID
   ├── Input: target distance from planner vs. actual distance from encoders (or gyro)
   ├── Output: desired forward speed for both motors
   └── kp/ki/kd determine how aggressively it corrects distance errors

3. heading_control PID
   ├── Input: target heading (0° for straight) vs. actual heading (encoders or gyro)
   ├── Output: differential speed offset (left vs. right)
   └── kp/ki/kd determine how aggressively it corrects heading errors

4. These two outputs are combined:
   ├── left_motor_speed  = forward_speed - heading_correction
   └── right_motor_speed = forward_speed + heading_correction

5. Individual Motor PID (for each motor)
   ├── Input: target speed from step 4 vs. actual motor speed from encoder
   ├── Output: duty cycle (PWM voltage)
   └── kp/ki/kd at the motor level determine how accurately each wheel tracks speed
When use_gyro(True) is active, the heading_control PID's feedback source switches from encoder-derived heading to IMU gyroscope heading. The distance_control still uses encoders since there's no independent distance sensor.
Practical Tuning
When to tune motor PID: Usually never. The defaults are well-calibrated for each motor type. You might reduce kp on a heavily geared mechanism to soften the response:
python# Softer hold for a crane arm under heavy load
arm = Motor(Port.E)
kp, ki, kd, deadzone, rate = arm.control.pid()
arm.control.pid(kp // 2, ki // 2, kd, deadzone, rate)
When to tune DriveBase PID: When your robot overshoots targets, oscillates at the end of moves, or drifts during straight-line driving. The settings() method handles the most common adjustments (speed and acceleration), but for fine control:
pythondb = DriveBase(left, right, 56, 112)
db.use_gyro(True)

# Make heading correction more aggressive (tighter straight-line driving)
kp, ki, kd, deadzone, rate = db.heading_control.pid()
db.heading_control.pid(kp * 2, ki, kd, deadzone, rate)

# Reduce distance overshoot
kp, ki, kd, deadzone, rate = db.distance_control.pid()
db.distance_control.pid(kp, ki, kd * 2, deadzone, rate)  # more damping
When to tune stall detection: When run_until_stalled() triggers too early (false positives) or too late (mechanism jams):
python# More sensitive stall detection for a delicate gripper
gripper = Motor(Port.C)
gripper.control.stall_tolerances(speed=20, time=200)
# Stalled if < 20 deg/s for > 200 ms at max torque
Bypassing the PID Entirely
If you want full manual control for your own control loop:

motor.dc(duty) — raw duty cycle, no PID at all
drive() with your own sensor loop — you're doing the outer-loop PID, the motor-level PID still runs underneath for speed tracking
track_target(angle) — bypasses the motion planner's acceleration profile but still uses the motor PID for position tracking

The architecture is essentially cascaded control: outer loop (DriveBase distance/heading) → inner loop (motor speed/position) → plant (PWM to motor). Each level has its own PID instance you can tune independently.where is the feedforward term applied12:26 PMSynthesized feedforward term history for comprehensive responseSynthesized feedforward term history for comprehensive responseGood question — the short answer is that the explicit feed_forward parameter was removed from the user API in v3.3+, but the concept of feedforward still exists inside the firmware. Here's the full picture.
What the Feed Forward Term Did (v2.0 through v3.0)
In the older API, feed_forward (percentage: %) added a feed forward signal to the PID feedback signal, in the direction of the speed reference. This value was expressed as a percentage of the absolute maximum duty cycle. Pybricks
The control law was:
actuation = kp * position_error
          + ki * integral(position_error)
          + kd * speed_error
          + feed_forward * sign(speed_reference) * max_duty
The feedforward term was applied at the motor level, summed directly into the duty cycle (PWM voltage) output alongside the PID feedback torque. It was keyed to the speed reference from the motion planner — not the actual measured speed. This is the critical distinction: feedforward acts on the command, not the error.
Conceptually, it was saying: "When the planner wants the motor to move forward, immediately apply X% of max voltage in that direction, then let the PID handle the residual error." This reduces the PID's workload during constant-velocity segments because the open-loop term carries the baseline load, and the PID only needs to correct deviations.
The default was 0% on most configurations, which means pure feedback control.
Why It Was Removed from the API
Starting around v3.3, the control.pid() signature changed to (kp, ki, kd, integral_deadzone, integral_rate) — the feed_forward parameter disappeared. The Pybricks firmware replaced the user-configurable feedforward percentage with an internal model-based approach.
The v3.x firmware includes a model subsystem on each motor — accessible via motor.model.state() which returns (angle, speed, current, stalled). This gets the estimated angle, speed, current, and stall state of the motor, using a simulation model that mimics the real motor. These estimates are updated faster than the real measurements. Pybricks
This internal motor model is essentially a more sophisticated feedforward. Instead of a constant percentage added in the direction of motion, the firmware uses a model of the motor's physics (back-EMF constant, friction, inertia) to predict the torque/voltage needed to follow the trajectory. The PID then only corrects the difference between the model's prediction and reality.
Where Feedforward Lives Now in the Architecture
The current v3.6 control loop at the motor level looks roughly like this:
Motion Planner → (position_ref, speed_ref, accel_ref)
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   Model-based FF    PID Feedback     Integral
        │                │                │
        │    kp*(pos_ref - pos_actual)    │
        │  + kd*(speed_ref - speed_actual)│
        │  + ki*integral(error)           │
        │                │                │
        └───────┬────────┘                │
                │                         │
                ▼                         │
        torque_ff + torque_pid + torque_i  │
                │                         │
                ▼                         │
        Clamp to control.limits(torque)   
                │
                ▼
        Convert torque → duty cycle (voltage)
                │
                ▼
            PWM to H-bridge
The model-based feedforward (torque_ff) is computed from the motor's physical parameters — it estimates the voltage needed to produce the desired speed given the motor's back-EMF constant, plus the torque needed for the desired acceleration given the rotor inertia, plus a friction compensation term. This is all internal to the C firmware and not exposed as a tunable parameter.
How This Interacts with DriveBase
At the DriveBase level, the feedforward situation is:

distance_control PID — computes a desired speed for the robot. No explicit feedforward at this level; it's pure PID feedback on distance error.
heading_control PID — computes a differential speed correction. Also pure PID feedback on heading error.
These produce per-motor speed targets — which become the setpoints for the individual motor controllers.
Each motor's controller — applies the internal model-based feedforward plus PID feedback to track that speed target.

So feedforward only exists at the innermost (motor) level of the cascade. The DriveBase-level controllers are pure feedback.
If You Need User-Configurable Feedforward
Since the explicit parameter is gone, your options are:
Option 1: Use dc() with your own feedforward
Bypass the PID entirely and implement your own control law:
python# Manual feedforward + feedback on a single motor
KFF = 0.05   # feedforward gain: duty per deg/s
KP = 0.1     # proportional gain

while True:
    speed_ref = 500  # desired speed
    speed_actual = motor.speed()
    error = speed_ref - speed_actual
    
    duty = KFF * speed_ref + KP * error  # feedforward + P feedback
    motor.dc(duty)
    wait(10)
Option 2: Use drive() with feedforward for DriveBase
Add a feedforward term to your outer control loop:
pythondb.use_gyro(True)

target_heading = 0
KFF_SPEED = 0.8    # fraction of desired speed to apply as open-loop
KP_HEADING = 2.0   # heading correction gain

desired_speed = 200  # mm/s

while db.distance() < 1000:
    heading_error = target_heading - hub.imu.heading()
    turn_correction = KP_HEADING * heading_error
    
    # The drive() command's speed argument acts as a kind of 
    # feedforward — you're commanding the desired speed directly,
    # and the internal motor PIDs track it
    db.drive(desired_speed, turn_correction)
    wait(10)
In this pattern, drive(speed, turn_rate) itself is the feedforward — you're telling the motors what speed you want, and the internal PID tracks it. Your outer loop only needs to handle the heading correction.
Option 3: Downgrade to v3.0 API
The v3.0 firmware still had the explicit feed_forward parameter. If you need it, you could run that firmware version, though you'd lose all the v3.3+ improvements.
The Bottom Line
The feedforward was always applied at the motor actuator level — summed into the duty cycle command alongside the PID output, keyed to the direction and magnitude of the speed reference from the motion planner. In v3.3+ it was internalized into a model-based predictor that the Pybricks team considers more robust than a user-tunable constant percentage. The tradeoff is that you lost the ability to tune it explicitly, but for most LEGO applications the model-based approach works better because it adapts to battery voltage and temperature changes automatically.



