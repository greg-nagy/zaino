//! Network type for Zaino configuration.

/// Network type for Zaino configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    /// Mainnet network
    Mainnet,
    /// Testnet network
    Testnet,
    /// Regtest network (for local testing)
    Regtest,
}

impl Network {
    /// Convert to Zebra's network type using default configurations.
    pub fn to_zebra_default(&self) -> zebra_chain::parameters::Network {
        self.into()
    }

    /// Convert to Zebra's network type for internal use (alias for to_zebra_default).
    pub fn to_zebra_network(&self) -> zebra_chain::parameters::Network {
        self.to_zebra_default()
    }

    /// Get the standard regtest activation heights used by Zaino.
    pub fn zaino_regtest_heights() -> zebra_chain::parameters::testnet::ConfiguredActivationHeights
    {
        zebra_chain::parameters::testnet::ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(1),
            sapling: Some(1),
            blossom: Some(1),
            heartwood: Some(1),
            canopy: Some(1),
            nu5: Some(1),
            nu6: Some(1),
            nu6_1: None,
            nu7: None,
        }
    }

    /// Determines if sync should be skipped for testing.
    ///
    /// - Mainnet/Testnet: Skip sync (false) because we don't want to sync real chains in tests
    /// - Regtest: Enable sync (true) because regtest is local and fast to sync
    pub fn should_sync_for_testing(&self) -> bool {
        match self {
            Network::Mainnet | Network::Testnet => false, // Real networks - don't sync in tests
            Network::Regtest => true,                     // Local network - safe and fast to sync
        }
    }
}

impl Into<zingo_infra_services::network::Network> for Network {
    fn into(self) -> zingo_infra_services::network::Network {
        match self {
            Network::Mainnet => zingo_infra_services::network::Network::Mainnet,
            Network::Regtest => zingo_infra_services::network::Network::Regtest,
            Network::Testnet => zingo_infra_services::network::Network::Testnet,
        }
    }
}

impl From<zingo_infra_services::network::Network> for Network {
    fn from(value: zingo_infra_services::network::Network) -> Self {
        match value {
            zingo_infra_services::network::Network::Regtest => Network::Regtest,
            zingo_infra_services::network::Network::Testnet => Network::Testnet,
            zingo_infra_services::network::Network::Mainnet => Network::Mainnet,
        }
    }
}

impl From<zebra_chain::parameters::Network> for Network {
    fn from(value: zebra_chain::parameters::Network) -> Self {
        match value {
            zebra_chain::parameters::Network::Mainnet => Network::Mainnet,
            zebra_chain::parameters::Network::Testnet(parameters) => {
                if parameters.is_regtest() {
                    Network::Regtest
                } else {
                    Network::Testnet
                }
            }
        }
    }
}

impl Into<zebra_chain::parameters::Network> for Network {
    fn into(self) -> zebra_chain::parameters::Network {
        match self {
            Network::Regtest => {
                zebra_chain::parameters::Network::new_regtest(Self::zaino_regtest_heights())
            }
            Network::Testnet => zebra_chain::parameters::Network::new_default_testnet(),
            Network::Mainnet => zebra_chain::parameters::Network::Mainnet,
        }
    }
}

impl Into<zebra_chain::parameters::Network> for &Network {
    fn into(self) -> zebra_chain::parameters::Network {
        (*self).into()
    }
}

impl Default for Network {
    fn default() -> Self {
        Network::Testnet
    }
}