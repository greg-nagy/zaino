//! Network type for Zaino configuration.

use serde::{Deserialize, Serialize};

/// Network type for Zaino configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    /// Mainnet network
    Mainnet,
    /// Testnet network
    Testnet,
    /// Regtest network (for local testing)
    Regtest(ActivationHeights),
}

/// Configurable activation heights for Regtest and configured Testnets.
///
/// We use our own type instead of the zebra type
/// as the zebra type is missing a number of useful
/// traits, notably Debug, PartialEq, and Eq
///
/// This also allows us to define our own set
/// of defaults
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct ActivationHeights {
    /// Activation height for `BeforeOverwinter` network upgrade.
    pub before_overwinter: Option<u32>,
    /// Activation height for `Overwinter` network upgrade.
    pub overwinter: Option<u32>,
    /// Activation height for `Sapling` network upgrade.
    pub sapling: Option<u32>,
    /// Activation height for `Blossom` network upgrade.
    pub blossom: Option<u32>,
    /// Activation height for `Heartwood` network upgrade.
    pub heartwood: Option<u32>,
    /// Activation height for `Canopy` network upgrade.
    pub canopy: Option<u32>,
    /// Activation height for `NU5` network upgrade.
    #[serde(rename = "NU5")]
    pub nu5: Option<u32>,
    /// Activation height for `NU6` network upgrade.
    #[serde(rename = "NU6")]
    pub nu6: Option<u32>,
    /// Activation height for `NU6.1` network upgrade.
    /// see https://zips.z.cash/#nu6-1-candidate-zips for info on NU6.1
    #[serde(rename = "NU6.1")]
    pub nu6_1: Option<u32>,
    /// Activation height for `NU7` network upgrade.
    #[serde(rename = "NU7")]
    pub nu7: Option<u32>,
}

impl Default for ActivationHeights {
    fn default() -> Self {
        ActivationHeights {
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
}

impl From<zebra_chain::parameters::testnet::ConfiguredActivationHeights> for ActivationHeights {
    fn from(
        zebra_chain::parameters::testnet::ConfiguredActivationHeights {
            before_overwinter,
            overwinter,
            sapling,
            blossom,
            heartwood,
            canopy,
            nu5,
            nu6,
            nu6_1,
            nu7,
        }: zebra_chain::parameters::testnet::ConfiguredActivationHeights,
    ) -> Self {
        Self {
            before_overwinter,
            overwinter,
            sapling,
            blossom,
            heartwood,
            canopy,
            nu5,
            nu6,
            nu6_1,
            nu7,
        }
    }
}
impl From<ActivationHeights> for zebra_chain::parameters::testnet::ConfiguredActivationHeights {
    fn from(
        ActivationHeights {
            before_overwinter,
            overwinter,
            sapling,
            blossom,
            heartwood,
            canopy,
            nu5,
            nu6,
            nu6_1,
            nu7,
        }: ActivationHeights,
    ) -> Self {
        Self {
            before_overwinter,
            overwinter,
            sapling,
            blossom,
            heartwood,
            canopy,
            nu5,
            nu6,
            nu6_1,
            nu7,
        }
    }
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
            Network::Regtest(_) => true,                  // Local network - safe and fast to sync
        }
    }
}

impl Into<zingo_infra_services::network::Network> for Network {
    fn into(self) -> zingo_infra_services::network::Network {
        match self {
            Network::Mainnet => zingo_infra_services::network::Network::Mainnet,
            Network::Regtest(_) => zingo_infra_services::network::Network::Regtest,
            Network::Testnet => zingo_infra_services::network::Network::Testnet,
        }
    }
}

impl From<zingo_infra_services::network::Network> for Network {
    fn from(value: zingo_infra_services::network::Network) -> Self {
        match value {
            zingo_infra_services::network::Network::Regtest => {
                Network::Regtest(ActivationHeights::default())
            }
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
                    let mut activation_heights = ActivationHeights {
                        before_overwinter: None,
                        overwinter: None,
                        sapling: None,
                        blossom: None,
                        heartwood: None,
                        canopy: None,
                        nu5: None,
                        nu6: None,
                        nu6_1: None,
                        nu7: None,
                    };
                    for (height, upgrade) in parameters.activation_heights().iter() {
                        match upgrade {
                            zebra_chain::parameters::NetworkUpgrade::Genesis => (),
                            zebra_chain::parameters::NetworkUpgrade::BeforeOverwinter => {
                                activation_heights.before_overwinter = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Overwinter => {
                                activation_heights.overwinter = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Sapling => {
                                activation_heights.sapling = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Blossom => {
                                activation_heights.blossom = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Heartwood => {
                                activation_heights.heartwood = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Canopy => {
                                activation_heights.canopy = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Nu5 => {
                                activation_heights.nu5 = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Nu6 => {
                                activation_heights.nu6 = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Nu6_1 => {
                                activation_heights.nu6_1 = Some(height.0)
                            }
                            zebra_chain::parameters::NetworkUpgrade::Nu7 => {
                                activation_heights.nu7 = Some(height.0)
                            }
                        }
                    }
                    Network::Regtest(activation_heights)
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
            Network::Regtest(activation_heights) => {
                zebra_chain::parameters::Network::new_regtest(activation_heights.into())
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
