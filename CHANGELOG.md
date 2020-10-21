# CHANGELOG

All notable changes to this project will be documented in this file.

---

## Table of Contents

<!-- vim-markdown-toc GFM -->

* [[v0.5.0] - 2020-10-21](#v050---2020-10-21)
   * [Added](#added)
* [[Unrealesed]](#unrealesed)
* [[v0.4.4] - 2020-04-28](#v044---2020-04-28)
   * [Changed](#changed)
* [[v0.4.3] - 2019-12-16](#v043---2019-12-16)
   * [Changed](#changed-1)
* [[v0.4.2] - 2019-11-26](#v042---2019-11-26)
   * [Added](#added-1)
   * [Changed](#changed-2)
* [[v0.4.1] - 2019-10-18](#v041---2019-10-18)
   * [Added](#added-2)
   * [Changed](#changed-3)
* [[v0.4.0] - 2019-10-12](#v040---2019-10-12)
   * [Added](#added-3)
   * [Changed](#changed-4)

<!-- vim-markdown-toc -->

---

<!-- Object info -->

---

## [v0.5.0] - 2020-10-21

### Added

- 添加conf/version.py文件，便于版本号管理
- 添加chitu与MQTT和TimescaleDB的通信

## [Unrealesed]

- update ziyan and chitu web api func (more like metrics method) to provide more information about app.
- support more message queue support like Pulsar

## [v0.4.4] - 2020-04-28

### Changed

- 修改`enque_script.lua`对heart beat的pack操作。
- 修改transport.py中，迭代过程中修改迭代对象的bug。

## [v0.4.3] - 2019-12-16

### Changed

- changed `enque_script.lua` to limit redis stream length

## [v0.4.2] - 2019-11-26

### Added

- add xtrim to limit data_stream maxlen when create data_stream

### Changed

- changed lua default MAXLEN to 100000
- fix finally bug

## [v0.4.1] - 2019-10-18

### Added

- changed redis data_queue list to data stream
- added ziyan lua redis produce to data_stream func
- added chitu consumer data_stream func
- added chitu consumer pending data_stream func to speed upload data when network recoverd
- remove etcd and confd support since these funcs were abandoned for long time

### Changed

- changed redis `dequeue` method to pop last element of `data_queue`

## [v0.4.0] - 2019-10-12

### Added

- added kafka interface support

### Changed

- changed redis `dequeue` method to pop last element of `data_queue`
