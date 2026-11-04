package com.zilliz.spark.connector

/** Offline field backfill from an existing V3 snapshot and external input:
  * validates fields and join keys, preserves physical row order, writes column
  * files and returns a result artifact. Its internal physical-row source reuses
  * the public snapshot table while retaining rows marked deleted. Snapshot
  * lifetime and online submission belong to the caller. Additional storage
  * adapters reuse this computation. Prepared calls can record their result on
  * the driver before owned resources close; that callback owns persistence, and
  * cleanup failures remain failures rather than permission to submit.
  */
package object backfill
