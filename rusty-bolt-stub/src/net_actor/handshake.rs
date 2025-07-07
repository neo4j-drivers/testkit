use std::borrow::Cow;
use std::io;

use anyhow::anyhow;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bolt_version::BoltCapabilities;
use crate::ext::fmt::IterFmtExt;
use crate::net_actor::logging::{debug, info, HasLoggingCtx, LoggingCtx};
use crate::net_actor::{cancelable_io, Connection, NetActor, NetActorError, NetActorResult};
use crate::parser::ActorConfig;
use crate::str_bytes;

impl<C: Connection> NetActor<'_, C> {
    pub(super) async fn handshake(&mut self) -> NetActorResult<()> {
        self.preamble().await?;
        if self.script.config.handshake.is_some() {
            return self.forced_handshake().await;
        }
        let res = match self.negotiate_client_version_request().await? {
            None => {
                swallow_anyhow_error(
                    self.logging_ctx(),
                    self.send_no_negotiated_bolt_version().await,
                )?;
                Err(NetActorError::from_anyhow(anyhow!(
                    "No comon bolt version found"
                )))
            }
            Some((MANIFEST_MAJOR, 1)) => {
                self.handshake_delay().await;
                self.perform_manifest_v1_negotiation().await
            }
            Some((MANIFEST_MAJOR, manifest)) => Err(NetActorError::from_anyhow(anyhow!(
                "Unimplemented manifest version {manifest}"
            ))),
            Some((major, minor)) => {
                if self.script.config.bolt_capabilities != Default::default() {
                    let msg = "Script contains bolt capabilities, \
                        but non-manifest style negotiation is used.";
                    debug!(self, "{msg}");
                    swallow_anyhow_error(
                        self.logging_ctx(),
                        self.send_no_negotiated_bolt_version().await,
                    )?;
                    Err(NetActorError::from_anyhow(anyhow!("{msg}")))
                } else if self.script.config.handshake_response.is_some() {
                    let msg = "Script contains hard-coded handshake response, \
                        but non-manifest style negotiation is used.";
                    debug!(self, "{msg}");
                    swallow_anyhow_error(
                        self.logging_ctx(),
                        self.send_no_negotiated_bolt_version().await,
                    )?;
                    Err(NetActorError::from_anyhow(anyhow!("{msg}")))
                } else {
                    self.handshake_delay().await;
                    self.perform_non_manifest_negotiation(major, minor).await
                }
            }
        };
        res.inspect_err(|e| debug!(self, "Handshake failed: {e}"))
    }

    async fn preamble(&mut self) -> NetActorResult<()> {
        let mut buffer = [0u8; 4];
        cancelable_io(
            "reading magic preamble",
            self.logging_ctx(),
            &self.ct,
            self.conn.read_exact(&mut buffer),
        )
        .await?;

        info!(self, "C: <MAGIC> {}", str_bytes::fmt_bytes_compact(&buffer));

        if buffer != [0x60, 0x60, 0xB0, 0x17] {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Received wrong magic preamble: {}",
                str_bytes::fmt_bytes(&buffer)
            )));
        }
        Ok(())
    }

    async fn negotiate_client_version_request(&mut self) -> NetActorResult<Option<(u8, u8)>> {
        let request = self.read_client_handshake_request().await?;

        for entry in request.chunks(4) {
            let entry = entry.try_into().unwrap();
            debug!(self, "Decoding client handshake request: {entry:?}");
            let agreement = ClientVersionRequest::from_bytes(entry)?
                .mask_reserved_bytes(&self.script.config)
                .negotiate(self.logging_ctx(), &self.script.config);
            if let Some(agreement) = agreement {
                return Ok(Some(agreement));
            }
        }
        Ok(None)
    }

    async fn read_client_handshake_request(&mut self) -> Result<[u8; 16], NetActorError> {
        let mut buffer = [0u8; 16];
        cancelable_io(
            "reading client handshake request",
            self.logging_ctx(),
            &self.ct,
            self.conn.read_exact(&mut buffer),
        )
        .await?;

        info!(
            self,
            "C: <HANDSHAKE> {}",
            buffer
                .chunks(4)
                .map(str_bytes::fmt_bytes_compact)
                .join_display(" ")
        );
        Ok(buffer)
    }

    async fn send_no_negotiated_bolt_version(&mut self) -> NetActorResult<()> {
        info!(self, "S: <HANDSHAKE> 0x00000000");
        cancelable_io(
            "sending no negotiated bolt version",
            self.logging_ctx(),
            &self.ct.clone(),
            async {
                self.conn.write_all(&[0x00, 0x00, 0x00, 0x00]).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn forced_handshake(&mut self) -> NetActorResult<()> {
        let handshake = self
            .script
            .config
            .handshake
            .as_ref()
            .expect("don't call forced_handshake without handshake");
        self.read_client_handshake_request().await?;

        info!(self, "S: <HANDSHAKE> {}", str_bytes::fmt_bytes(handshake));

        cancelable_io(
            "sending forced handshake",
            self.logging_ctx(),
            &self.ct,
            async {
                self.conn.write_all(handshake).await?;
                self.conn.flush().await
            },
        )
        .await?;

        let Some(expected_response) = &self.script.config.handshake_response else {
            return Ok(());
        };

        let mut received_response = vec![0; expected_response.len()];
        cancelable_io(
            "reading forced handshake response",
            self.logging_ctx(),
            &self.ct,
            self.conn.read_exact(&mut received_response),
        )
        .await?;

        info!(
            self,
            "C: <HANDSHAKE> {}",
            str_bytes::fmt_bytes(&received_response)
        );

        if expected_response != &received_response {
            let msg = format!(
                "Received handshake response ({}) doesn't match expected response ({})",
                str_bytes::fmt_bytes(&received_response),
                str_bytes::fmt_bytes(expected_response)
            );
            debug!(self, "{msg}");
            return Err(NetActorError::from_anyhow(anyhow!("{msg}")));
        }
        Ok(())
    }

    async fn perform_non_manifest_negotiation(
        &mut self,
        major: u8,
        minor: u8,
    ) -> NetActorResult<()> {
        let response: Cow<[u8]> = match &self.script.config.handshake {
            None => Cow::Owned(vec![0x00, 0x00, minor, major]),
            Some(response) => Cow::Borrowed(response),
        };
        info!(
            self,
            "S: <HANDSHAKE> {}",
            str_bytes::fmt_bytes_compact(&response)
        );
        cancelable_io(
            "sending non-manifest negotiated bolt version",
            self.logging_ctx(),
            &self.ct.clone(),
            async {
                self.conn.write_all(&response).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn perform_manifest_v1_negotiation(&mut self) -> NetActorResult<()> {
        self.write_manifest_v1_offer().await?;
        self.read_manifest_v1_response().await
    }

    async fn write_manifest_v1_offer(&mut self) -> NetActorResult<()> {
        let version = self.script.config.bolt_version_raw;
        let version_aliases = self
            .script
            .config
            .bolt_version
            .backwards_equivalent_versions();
        let versions: Vec<_> = [version]
            .into_iter()
            .chain(version_aliases.iter().copied())
            .map(|(major, minor)| [0, 0, minor, major])
            .collect();
        let capabilities = self.script.config.bolt_capabilities.raw();
        info!(
            self,
            "S: <HANDSHAKE> 0x000001FF [{}] {} {}",
            versions.len(),
            versions
                .iter()
                .map(|b| str_bytes::fmt_bytes_compact(b))
                .join_display(" "),
            str_bytes::fmt_bytes_compact(capabilities)
        );
        cancelable_io(
            "sending manifest v1 style server offer",
            self.logging_ctx(),
            &self.ct.clone(),
            async {
                self.conn.write_all(&[0x00, 0x00, 0x01, 0xFF]).await?;
                self.write_var_int(versions.len()).await?;
                for version in versions {
                    self.conn.write_all(&version).await?;
                }
                self.conn.write_all(capabilities).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn read_manifest_v1_response(&mut self) -> NetActorResult<()> {
        let mut version_choice = [0u8; 4];
        cancelable_io(
            "reading manifest v1 version choice",
            self.logging_ctx(),
            &self.ct,
            self.conn.read_exact(&mut version_choice),
        )
        .await?;
        let raw_capabilities = cancelable_io(
            "reading manifest v1 capabilities",
            self.logging_ctx(),
            &self.ct.clone(),
            self.read_capabilities(),
        )
        .await?;

        info!(
            self,
            "C: <HANDSHAKE> {} {}",
            str_bytes::fmt_bytes_compact(&version_choice),
            str_bytes::fmt_bytes_compact(&raw_capabilities)
        );

        ClientVersionRequest::from_bytes(version_choice)?
            .accept_exact_version(self.logging_ctx(), &self.script.config)?;
        let capabilities = BoltCapabilities::from_bytes(raw_capabilities).map_err(|e| {
            NetActorError::from_anyhow(anyhow!("Failed to parse client capabilities: {e}"))
        })?;
        if capabilities != self.script.config.bolt_capabilities {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Client capabilities don't match expected capabilities"
            )));
        }
        Ok(())
    }

    async fn write_var_int(&mut self, mut val: usize) -> io::Result<()> {
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
            }

            self.conn.write_all(&[byte]).await?;
            if val == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn read_capabilities(&mut self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(self.script.config.bolt_capabilities.raw().len());
        let mut byte = [0u8; 1];
        loop {
            self.conn.read_exact(&mut byte).await?;
            bytes.push(byte[0]);
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        Ok(bytes)
    }

    async fn handshake_delay(&self) {
        let Some(delay) = self.script.config.handshake_delay else {
            return;
        };
        info!(self, "S: <HANDSHAKE DELAY> {}s", delay.as_secs_f64());
        tokio::time::sleep(delay).await;
    }
}

const MANIFEST_MAJOR: u8 = 0xFF;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
struct ClientVersionRequest {
    major: u8,
    minor: u8,
    range: u8,
}

impl ClientVersionRequest {
    fn new(major: u8, minor: u8, range: u8) -> NetActorResult<Self> {
        if range > minor {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Range ({range}) may not be greater than minor ({minor})",
            )));
        }
        Ok(Self {
            major,
            minor,
            range,
        })
    }

    fn from_bytes(bytes: [u8; 4]) -> NetActorResult<Self> {
        let [_, range, minor, major] = bytes;
        Self::new(major, minor, range)
    }

    fn mask_reserved_bytes(&self, actor_config: &ActorConfig) -> Self {
        let mut request = *self;
        if actor_config.handshake_manifest_version.unwrap_or_default() != 0 {
            // forced handshake-manifest style negotiation => use reserved bytes
            return request;
        }

        if !actor_config.bolt_version.supports_range() {
            request.range = 0;
        }
        if !actor_config.bolt_version.supports_minor() {
            request.minor = 0;
        }
        request
    }

    fn negotiate(&self, logging_ctx: LoggingCtx, actor_config: &ActorConfig) -> Option<(u8, u8)> {
        match (actor_config.handshake_manifest_version, self.major) {
            (None, MANIFEST_MAJOR) => {
                let requested_min = self.minor - self.range;
                let requested_max = self.minor;
                let max = actor_config.bolt_version.max_handshake_manifest_version();
                debug!(
                    logging_ctx,
                    "Manifest style request {}",
                    if requested_min == requested_max {
                        format!("v{requested_min}")
                    } else {
                        format!("v{requested_min}-v{requested_max}")
                    }
                );
                match max {
                    0 => {
                        debug!(
                            logging_ctx,
                            "Server manifest max v0 => no manifest support => reject"
                        );
                        None
                    }
                    _ if max < requested_min => {
                        debug!(
                            logging_ctx,
                            "Server manifest max v{max} lower than requested minimum => reject"
                        );
                        None
                    }
                    _ if max < requested_max => {
                        debug!(
                            logging_ctx,
                            "Server manifest max v{max} is lower than requested maximum => \
                            accept server maximum"
                        );
                        Some((MANIFEST_MAJOR, max))
                    }
                    _ => {
                        debug!(
                            logging_ctx,
                            "Server manifest max v{max} is higher  than or equals requested \
                            maximum => accept requested maximum"
                        );
                        Some((MANIFEST_MAJOR, requested_max))
                    }
                }
            }
            (Some(0), MANIFEST_MAJOR) => {
                debug!(
                    logging_ctx,
                    "Non-manifest style enforced => reject manifest style request"
                );
                None
            }
            (None, _) | (Some(0), _) => {
                let (server_major, server_minor) = actor_config.bolt_version_raw;
                if self.matches(server_major, server_minor) {
                    debug!(
                        logging_ctx,
                        "Non-manifest style request matches server version {:?} => accept",
                        actor_config.bolt_version_raw
                    );
                    return Some((server_major, server_minor));
                }
                for alias in actor_config.bolt_version.backwards_equivalent_versions() {
                    if self.matches(alias.0, alias.1) {
                        debug!(
                            logging_ctx,
                            "Non-manifest style request matches server alias {alias:?} => accept",
                        );
                        return Some(*alias);
                    }
                }
                debug!(
                    logging_ctx,
                    "Non-manifest style request doesn't match server version {:?} \
                    or aliases {:?} => reject",
                    actor_config.bolt_version_raw,
                    actor_config.bolt_version.backwards_equivalent_versions(),
                );
                None
            }
            (Some(forced_manifest), MANIFEST_MAJOR) => {
                match self.matches(MANIFEST_MAJOR, forced_manifest) {
                    true => {
                        debug!(
                            logging_ctx,
                            "Enforced manifest style v{forced_manifest} matches => accept"
                        );
                        Some((MANIFEST_MAJOR, forced_manifest))
                    }
                    false => {
                        debug!(
                            logging_ctx,
                            "Enforced manifest style v{forced_manifest} doesn't matches => reject"
                        );
                        None
                    }
                }
            }
            (Some(forced_manifest), _) => {
                debug!(
                    logging_ctx,
                    "Enforced manifest style v{forced_manifest} => \
                    reject non-manifest style request"
                );
                None
            }
        }
    }

    fn accept_exact_version(
        &self,
        logging_ctx: LoggingCtx,
        actor_config: &ActorConfig,
    ) -> NetActorResult<()> {
        if self.range != 0 {
            debug!(
                logging_ctx,
                "Rejecting version as it contains a range: {self:?}"
            );
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version, got range {self:?}",
            )));
        }
        if self.major == MANIFEST_MAJOR {
            debug!(
                logging_ctx,
                "Rejecting version as it is a manifest marker: {self:?}"
            );
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version, got manifest version {self:?}",
            )));
        }
        let (major, minor) = actor_config.bolt_version_raw;
        if self.major != major || self.minor != minor {
            debug!(
                logging_ctx,
                "Rejecting version as it doesn't match server version {:?}: {self:?}",
                actor_config.bolt_version_raw
            );
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version {:?}, got {self:?}",
                actor_config.bolt_version_raw,
            )));
        }
        Ok(())
    }

    fn matches(&self, major: u8, minor: u8) -> bool {
        self.major == major && (self.minor.saturating_sub(self.range)..=self.minor).contains(&minor)
    }
}

fn swallow_anyhow_error(logging_ctx: LoggingCtx, res: NetActorResult<()>) -> NetActorResult<()> {
    match res {
        Err(NetActorError::Anyhow(e)) => {
            debug!(logging_ctx, "Swallowed IO error: {e:#}");
            Ok(())
        }
        res => res,
    }
}
