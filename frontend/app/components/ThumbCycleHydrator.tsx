'use client';

import { useEffect } from 'react';
import { attachThumbCycle } from './thumbCycle';

export function ThumbCycleHydrator() {
  useEffect(() => {
    attachThumbCycle();
  }, []);
  return null;
}
