// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{fmt::Debug, sync::Arc};

#[cfg(feature = "tracing")]
use fastrace::prelude::*;
use foyer_common::{
    error::{Error, Result},
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::io::{
    bytes::{IoB, IoBuf, IoBufMut, Raw},
    device::Partition,
    engine::{IoEngine, IoEngineBuildContext, IoEngineConfig, IoHandle},
};

#[cfg(target_family = "windows")]
use std::{
    fs::File,
    ops::{Deref, DerefMut},
};

#[cfg(target_family = "windows")]
use crate::RawFile;

#[cfg(target_family = "windows")]
#[derive(Debug)]
struct FileHandle(File);

#[cfg(target_family = "windows")]
impl TryFrom<RawFile> for FileHandle {
    type Error = Error;

    fn try_from(raw: RawFile) -> Result<Self> {
        use std::{os::windows::io::FromRawHandle, ptr};

        use windows_sys::Win32::{
            Foundation::HANDLE,
            System::Threading::{DUPLICATE_SAME_ACCESS, DuplicateHandle, GetCurrentProcess},
        };

        let mut duplicate: HANDLE = ptr::null_mut();
        let ok = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                raw.0 as HANDLE,
                GetCurrentProcess(),
                &mut duplicate,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(Error::io_error(std::io::Error::last_os_error()));
        }

        let file = unsafe { File::from_raw_handle(duplicate as _) };
        Ok(Self(file))
    }
}

#[cfg(target_family = "windows")]
impl Deref for FileHandle {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(target_family = "windows")]
impl DerefMut for FileHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(target_family = "unix")]
fn read_exact_at(fd: std::os::fd::RawFd, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    use std::io::ErrorKind;

    while !buf.is_empty() {
        let res = unsafe { libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), offset as _) };
        if res < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(Error::io_error(err));
        }

        if res == 0 {
            return Err(Error::io_error(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )));
        }

        let read = res as usize;
        buf = &mut buf[read..];
        offset += read as u64;
    }

    Ok(())
}

#[cfg(target_family = "unix")]
fn write_all_at(fd: std::os::fd::RawFd, mut buf: &[u8], mut offset: u64) -> Result<()> {
    use std::io::ErrorKind;

    while !buf.is_empty() {
        let res = unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), offset as _) };
        if res < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(Error::io_error(err));
        }

        if res == 0 {
            return Err(Error::io_error(std::io::Error::new(
                ErrorKind::WriteZero,
                "failed to write the buffer",
            )));
        }

        let written = res as usize;
        buf = &buf[written..];
        offset += written as u64;
    }

    Ok(())
}

/// Config for synchronous I/O engine with pread(2)/pwrite(2).
#[derive(Debug)]
pub struct PsyncIoEngineConfig {
    #[cfg(any(test, feature = "test_utils"))]
    write_io_latency: Option<std::ops::Range<std::time::Duration>>,

    #[cfg(any(test, feature = "test_utils"))]
    read_io_latency: Option<std::ops::Range<std::time::Duration>>,
}

impl Default for PsyncIoEngineConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<PsyncIoEngineConfig> for Box<dyn IoEngineConfig> {
    fn from(builder: PsyncIoEngineConfig) -> Self {
        builder.boxed()
    }
}

impl PsyncIoEngineConfig {
    /// Create a new synchronous I/O engine config with default configurations.
    pub fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test_utils"))]
            write_io_latency: None,
            #[cfg(any(test, feature = "test_utils"))]
            read_io_latency: None,
        }
    }

    /// Set the simulated additional write I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_write_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.write_io_latency = Some(latency);
        self
    }

    /// Set the simulated additional read I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_read_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.read_io_latency = Some(latency);
        self
    }
}

impl IoEngineConfig for PsyncIoEngineConfig {
    fn build(self: Box<Self>, ctx: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>> {
        async move {
            let engine = PsyncIoEngine {
                spawner: ctx.spawner,
                #[cfg(any(test, feature = "test_utils"))]
                write_io_latency: self.write_io_latency,
                #[cfg(any(test, feature = "test_utils"))]
                read_io_latency: self.read_io_latency,
            };
            let engine: Arc<dyn IoEngine> = Arc::new(engine);
            Ok(engine)
        }
        .boxed()
    }
}

/// The synchronous I/O engine that uses pread(2)/pwrite(2) and tokio thread pool for reading and writing.
pub struct PsyncIoEngine {
    spawner: Spawner,

    #[cfg(any(test, feature = "test_utils"))]
    write_io_latency: Option<std::ops::Range<std::time::Duration>>,
    #[cfg(any(test, feature = "test_utils"))]
    read_io_latency: Option<std::ops::Range<std::time::Duration>>,
}

impl Debug for PsyncIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PsyncIoEngine").finish()
    }
}

impl IoEngine for PsyncIoEngine {
    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::psync::read")
    )]
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (raw, offset) = partition.translate(offset);
        let runtime = self.spawner.clone();

        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::psync::read::io");

        #[cfg(any(test, feature = "test_utils"))]
        let read_io_latency = self.read_io_latency.clone();
        async move {
            #[cfg(target_family = "windows")]
            let file = match FileHandle::try_from(raw) {
                Ok(file) => file,
                Err(e) => {
                    let buf: Box<dyn IoB> = buf.into_iob();
                    return (buf, Err(e));
                }
            };

            #[cfg(target_family = "unix")]
            let fd = raw.0;

            let (buf, res) = match runtime
                .spawn_blocking(move || {
                    let (ptr, len) = buf.as_raw_parts();
                    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                    let res = {
                        #[cfg(target_family = "windows")]
                        {
                            use std::os::windows::fs::FileExt;
                            file.seek_read(slice, offset).map(|_| ()).map_err(Error::io_error)
                        }
                        #[cfg(target_family = "unix")]
                        {
                            read_exact_at(fd, slice, offset)
                        }
                    };
                    #[cfg(any(test, feature = "test_utils"))]
                    if let Some(lat) = read_io_latency {
                        std::thread::sleep(rand::random_range(lat));
                    }
                    (buf, res)
                })
                .await
            {
                Ok((buf, res)) => {
                    #[cfg(feature = "tracing")]
                    drop(span);
                    (buf, res)
                }
                Err(e) => return (Box::new(Raw::new(0)) as Box<dyn IoB>, Err(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::psync::write")
    )]
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (raw, offset) = partition.translate(offset);
        let runtime = self.spawner.clone();

        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::psync::write::io");

        #[cfg(any(test, feature = "test_utils"))]
        let write_io_latency = self.write_io_latency.clone();
        async move {
            #[cfg(target_family = "windows")]
            let file = match FileHandle::try_from(raw) {
                Ok(file) => file,
                Err(e) => {
                    let buf: Box<dyn IoB> = buf.into_iob();
                    return (buf, Err(e));
                }
            };

            #[cfg(target_family = "unix")]
            let fd = raw.0;

            let (buf, res) = match runtime
                .spawn_blocking(move || {
                    let (ptr, len) = buf.as_raw_parts();
                    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
                    let res = {
                        #[cfg(target_family = "windows")]
                        {
                            use std::os::windows::fs::FileExt;
                            file.seek_write(slice, offset).map(|_| ()).map_err(Error::io_error)
                        }
                        #[cfg(target_family = "unix")]
                        {
                            write_all_at(fd, slice, offset)
                        }
                    };
                    #[cfg(any(test, feature = "test_utils"))]
                    if let Some(lat) = write_io_latency {
                        std::thread::sleep(rand::random_range(lat));
                    }
                    (buf, res)
                })
                .await
            {
                Ok((buf, res)) => {
                    #[cfg(feature = "tracing")]
                    drop(span);
                    (buf, res)
                }
                Err(e) => return (Box::new(Raw::new(0)) as Box<dyn IoB>, Err(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        time::{Duration, Instant},
    };

    use rand::{Fill, rng};
    use tempfile::tempdir;

    use super::*;
    use crate::io::{
        bytes::IoSliceMut,
        device::{Device, DeviceBuilder, file::FileDeviceBuilder},
        engine::IoEngineBuildContext,
    };

    fn build_test_file_device(path: impl AsRef<Path>) -> Result<Arc<dyn Device>> {
        let device = FileDeviceBuilder::new(&path).with_capacity(16 * 1024 * 1024).build()?;
        for _ in 0..16 {
            device.create_partition(1024 * 1024)?;
        }
        Ok(device)
    }

    #[test_log::test(tokio::test)]
    async fn test_psync_io_latency_is_applied() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_file_latency");
        let device = build_test_file_device(&path).unwrap();
        let engine = PsyncIoEngineConfig::new()
            .with_write_io_latency(Duration::from_millis(50)..Duration::from_millis(60))
            .with_read_io_latency(Duration::from_millis(50)..Duration::from_millis(60))
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();

        let mut b1 = Box::new(IoSliceMut::new(16 * 1024));
        Fill::fill_slice(&mut b1[..], &mut rng());

        let started = Instant::now();
        let (b1, res) = engine.write(b1, device.partition(0).as_ref(), 0).await;
        res.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(40));
        let b1 = b1.try_into_io_slice_mut().unwrap();

        let b2 = Box::new(IoSliceMut::new(16 * 1024));
        let started = Instant::now();
        let (b2, res) = engine.read(b2, device.partition(0).as_ref(), 0).await;
        res.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(40));
        let b2 = b2.try_into_io_slice_mut().unwrap();
        assert_eq!(b1, b2);
    }
}
