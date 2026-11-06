package com.zilliz.spark.connector

/** Public read and storage option parsing. MilvusOption describes snapshot
  * selection, projections, scalar filters and read limits. StorageOptions and
  * HadoopStorageKeys resolve per-input storage settings. OptionParsing provides
  * strict shared value parsing; TaskResources describes executor resources and
  * stage-level task core allocation.
  */
package object options
