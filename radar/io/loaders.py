#!/usr/bin/env python3
import base64
from collections import Counter
import numpy as np


def convert_audio_data(x):
    return np.array(base64.b64decode(''.join(x.split('\\n')))
                    .split(b'\n')[1].split(b';')[1:], dtype=float)
