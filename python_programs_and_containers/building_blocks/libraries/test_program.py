
import time
import random
import sys

while True:
    time.sleep(random.randint(5, 15))
    if random.random() < 0.3:
        print("Random error occurred!", file=sys.stderr)
        sys.exit(1)
    print("Still running...", file=sys.stderr)
