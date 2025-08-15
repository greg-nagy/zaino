//! Transaction fetching and deserialization functionality.

use crate::chain::{
    error::ParseError,
    utils::{read_bytes, read_i64, read_u32, read_u64, skip_bytes, CompactSize, ParseFromSlice},
};
use std::io::Cursor;
use zaino_proto::proto::compact_formats::{
    CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend, CompactTx,
};

/// Txin format as described in <https://en.bitcoin.it/wiki/Transaction>
#[derive(Debug, Clone)]
pub struct TxIn {
    // PrevTxHash - Size\[bytes\]: 32
    prev_txid: Vec<u8>,
    // PrevTxOutIndex - Size\[bytes\]: 4
    prev_index: u32,
    /// CompactSize-prefixed, could be a pubkey or a script
    ///
    /// Size\[bytes\]: CompactSize
    script_sig: Vec<u8>,
    // SequenceNumber \[IGNORED\] - Size\[bytes\]: 4
}

impl TxIn {
    fn into_inner(self) -> (Vec<u8>, u32, Vec<u8>) {
        (self.prev_txid, self.prev_index, self.script_sig)
    }
}

impl ParseFromSlice for TxIn {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for TxIn::parse_from_slice".to_string(),
            ));
        }
        if tx_version.is_some() {
            return Err(ParseError::InvalidData(
                "tx_version must be None for TxIn::parse_from_slice".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        let prev_txid = read_bytes(&mut cursor, 32, "Error reading TxIn::PrevTxHash")?;
        let prev_index = read_u32(&mut cursor, "Error reading TxIn::PrevTxOutIndex")?;
        let script_sig = {
            let compact_length = CompactSize::read(&mut cursor)?;
            read_bytes(
                &mut cursor,
                compact_length as usize,
                "Error reading TxIn::ScriptSig",
            )?
        };
        skip_bytes(&mut cursor, 4, "Error skipping TxIn::SequenceNumber")?;

        Ok((
            &data[cursor.position() as usize..],
            TxIn {
                prev_txid,
                prev_index,
                script_sig,
            },
        ))
    }
}

/// Txout format as described in <https://en.bitcoin.it/wiki/Transaction>
#[derive(Debug, Clone)]
pub struct TxOut {
    /// Non-negative int giving the number of zatoshis to be transferred
    ///
    /// Size\[bytes\]: 8
    value: u64,
    // Script - Size\[bytes\]: CompactSize
    script_hash: Vec<u8>,
}

impl TxOut {
    fn into_inner(self) -> (u64, Vec<u8>) {
        (self.value, self.script_hash)
    }
}

impl ParseFromSlice for TxOut {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for TxOut::parse_from_slice".to_string(),
            ));
        }
        if tx_version.is_some() {
            return Err(ParseError::InvalidData(
                "tx_version must be None for TxOut::parse_from_slice".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        let value = read_u64(&mut cursor, "Error TxOut::reading Value")?;
        let script_hash = {
            let compact_length = CompactSize::read(&mut cursor)?;
            read_bytes(
                &mut cursor,
                compact_length as usize,
                "Error reading TxOut::ScriptHash",
            )?
        };

        Ok((
            &data[cursor.position() as usize..],
            TxOut { script_hash, value },
        ))
    }
}

#[allow(clippy::type_complexity)]
fn parse_transparent(data: &[u8]) -> Result<(&[u8], Vec<TxIn>, Vec<TxOut>), ParseError> {
    let mut cursor = Cursor::new(data);

    let tx_in_count = CompactSize::read(&mut cursor)?;
    let mut tx_ins = Vec::with_capacity(tx_in_count as usize);
    for _ in 0..tx_in_count {
        let (remaining_data, tx_in) =
            TxIn::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
        tx_ins.push(tx_in);
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
    }
    let tx_out_count = CompactSize::read(&mut cursor)?;
    let mut tx_outs = Vec::with_capacity(tx_out_count as usize);
    for _ in 0..tx_out_count {
        let (remaining_data, tx_out) =
            TxOut::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
        tx_outs.push(tx_out);
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
    }

    Ok((&data[cursor.position() as usize..], tx_ins, tx_outs))
}

/// Spend is a Sapling Spend Description as described in 7.3 of the Zcash
/// protocol specification.
#[derive(Debug, Clone)]
pub struct Spend {
    // Cv \[IGNORED\] - Size\[bytes\]: 32
    // Anchor \[IGNORED\] - Size\[bytes\]: 32
    /// A nullifier to a sapling note.
    ///
    /// Size\[bytes\]: 32
    nullifier: Vec<u8>,
    // Rk \[IGNORED\] - Size\[bytes\]: 32
    // Zkproof \[IGNORED\] - Size\[bytes\]: 192
    // SpendAuthSig \[IGNORED\] - Size\[bytes\]: 64
}

impl Spend {
    fn into_inner(self) -> Vec<u8> {
        self.nullifier
    }
}

impl ParseFromSlice for Spend {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for Spend::parse_from_slice".to_string(),
            ));
        }
        let tx_version = tx_version.ok_or_else(|| {
            ParseError::InvalidData(
                "tx_version must be used for Spend::parse_from_slice".to_string(),
            )
        })?;
        let mut cursor = Cursor::new(data);

        skip_bytes(&mut cursor, 32, "Error skipping Spend::Cv")?;
        if tx_version <= 4 {
            skip_bytes(&mut cursor, 32, "Error skipping Spend::Anchor")?;
        }
        let nullifier = read_bytes(&mut cursor, 32, "Error reading Spend::nullifier")?;
        skip_bytes(&mut cursor, 32, "Error skipping Spend::Rk")?;
        if tx_version <= 4 {
            skip_bytes(&mut cursor, 192, "Error skipping Spend::Zkproof")?;
            skip_bytes(&mut cursor, 64, "Error skipping Spend::SpendAuthSig")?;
        }

        Ok((&data[cursor.position() as usize..], Spend { nullifier }))
    }
}

/// output is a Sapling Output Description as described in section 7.4 of the
/// Zcash protocol spec.
#[derive(Debug, Clone)]
pub struct Output {
    // Cv \[IGNORED\] - Size\[bytes\]: 32
    /// U-coordinate of the note commitment, derived from the note's value, recipient, and a
    /// random value.
    ///
    /// Size\[bytes\]: 32
    cmu: Vec<u8>,
    /// Ephemeral public key for Diffie-Hellman key exchange.
    ///
    /// Size\[bytes\]: 32
    ephemeral_key: Vec<u8>,
    /// Encrypted transaction details including value transferred and an optional memo.
    ///
    /// Size\[bytes\]: 580
    enc_ciphertext: Vec<u8>,
    // OutCiphertext \[IGNORED\] - Size\[bytes\]: 80
    // Zkproof \[IGNORED\] - Size\[bytes\]: 192
}

impl Output {
    fn into_parts(self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (self.cmu, self.ephemeral_key, self.enc_ciphertext)
    }
}

impl ParseFromSlice for Output {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for Output::parse_from_slice".to_string(),
            ));
        }
        let tx_version = tx_version.ok_or_else(|| {
            ParseError::InvalidData(
                "tx_version must be used for Output::parse_from_slice".to_string(),
            )
        })?;
        let mut cursor = Cursor::new(data);

        skip_bytes(&mut cursor, 32, "Error skipping Output::Cv")?;
        let cmu = read_bytes(&mut cursor, 32, "Error reading Output::cmu")?;
        let ephemeral_key = read_bytes(&mut cursor, 32, "Error reading Output::ephemeral_key")?;
        let enc_ciphertext = read_bytes(&mut cursor, 580, "Error reading Output::enc_ciphertext")?;
        skip_bytes(&mut cursor, 80, "Error skipping Output::OutCiphertext")?;
        if tx_version <= 4 {
            skip_bytes(&mut cursor, 192, "Error skipping Output::Zkproof")?;
        }

        Ok((
            &data[cursor.position() as usize..],
            Output {
                cmu,
                ephemeral_key,
                enc_ciphertext,
            },
        ))
    }
}

/// joinSplit is a JoinSplit description as described in 7.2 of the Zcash
/// protocol spec. Its exact contents differ by transaction version and network
/// upgrade level. Only version 4 is supported, no need for proofPHGR13.
///
/// NOTE: Legacy, no longer used but included for consistency.
#[derive(Debug, Clone)]
struct JoinSplit {
    //vpubOld \[IGNORED\] - Size\[bytes\]: 8
    //vpubNew \[IGNORED\] - Size\[bytes\]: 8
    //anchor \[IGNORED\] - Size\[bytes\]: 32
    //nullifiers \[IGNORED\] - Size\[bytes\]: 64/32
    //commitments \[IGNORED\] - Size\[bytes\]: 64/32
    //ephemeralKey \[IGNORED\] - Size\[bytes\]: 32
    //randomSeed \[IGNORED\] - Size\[bytes\]: 32
    //vmacs \[IGNORED\] - Size\[bytes\]: 64/32
    //proofGroth16 \[IGNORED\] - Size\[bytes\]: 192
    //encCiphertexts \[IGNORED\] - Size\[bytes\]: 1202
}

impl ParseFromSlice for JoinSplit {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for JoinSplit::parse_from_slice".to_string(),
            ));
        }
        if tx_version.is_some() {
            return Err(ParseError::InvalidData(
                "tx_version must be None for JoinSplit::parse_from_slice".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        skip_bytes(&mut cursor, 8, "Error skipping JoinSplit::vpubOld")?;
        skip_bytes(&mut cursor, 8, "Error skipping JoinSplit::vpubNew")?;
        skip_bytes(&mut cursor, 32, "Error skipping JoinSplit::anchor")?;
        skip_bytes(&mut cursor, 64, "Error skipping JoinSplit::nullifiers")?;
        skip_bytes(&mut cursor, 64, "Error skipping JoinSplit::commitments")?;
        skip_bytes(&mut cursor, 32, "Error skipping JoinSplit::ephemeralKey")?;
        skip_bytes(&mut cursor, 32, "Error skipping JoinSplit::randomSeed")?;
        skip_bytes(&mut cursor, 64, "Error skipping JoinSplit::vmacs")?;
        skip_bytes(&mut cursor, 192, "Error skipping JoinSplit::proofGroth16")?;
        skip_bytes(
            &mut cursor,
            1202,
            "Error skipping JoinSplit::encCiphertexts",
        )?;

        Ok((&data[cursor.position() as usize..], JoinSplit {}))
    }
}

/// An Orchard action.
#[derive(Debug, Clone)]
struct Action {
    // Cv \[IGNORED\] - Size\[bytes\]: 32
    /// A nullifier to a orchard note.
    ///
    /// Size\[bytes\]: 32
    nullifier: Vec<u8>,
    // Rk \[IGNORED\] - Size\[bytes\]: 32
    /// X-coordinate of the commitment to the note.
    ///
    /// Size\[bytes\]: 32
    cmx: Vec<u8>,
    /// Ephemeral public key.
    ///
    /// Size\[bytes\]: 32
    ephemeral_key: Vec<u8>,
    /// Encrypted details of the new note, including its value and recipient's data.
    ///
    /// Size\[bytes\]: 580
    enc_ciphertext: Vec<u8>,
    // OutCiphertext \[IGNORED\] - Size\[bytes\]: 80
}

impl Action {
    fn into_parts(self) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.nullifier,
            self.cmx,
            self.ephemeral_key,
            self.enc_ciphertext,
        )
    }
}

impl ParseFromSlice for Action {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        if txid.is_some() {
            return Err(ParseError::InvalidData(
                "txid must be None for Action::parse_from_slice".to_string(),
            ));
        }
        if tx_version.is_some() {
            return Err(ParseError::InvalidData(
                "tx_version must be None for Action::parse_from_slice".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        skip_bytes(&mut cursor, 32, "Error skipping Action::Cv")?;
        let nullifier = read_bytes(&mut cursor, 32, "Error reading Action::nullifier")?;
        skip_bytes(&mut cursor, 32, "Error skipping Action::Rk")?;
        let cmx = read_bytes(&mut cursor, 32, "Error reading Action::cmx")?;
        let ephemeral_key = read_bytes(&mut cursor, 32, "Error reading Action::ephemeral_key")?;
        let enc_ciphertext = read_bytes(&mut cursor, 580, "Error reading Action::enc_ciphertext")?;
        skip_bytes(&mut cursor, 80, "Error skipping Action::OutCiphertext")?;

        Ok((
            &data[cursor.position() as usize..],
            Action {
                nullifier,
                cmx,
                ephemeral_key,
                enc_ciphertext,
            },
        ))
    }
}

/// Full Zcash transaction data.
#[derive(Debug, Clone)]
struct TransactionData {
    /// Indicates if the transaction is an Overwinter-enabled transaction.
    ///
    /// Size\[bytes\]: [in 4 byte header]
    f_overwintered: bool,
    /// The transaction format version.
    ///
    /// Size\[bytes\]: [in 4 byte header]
    version: u32,
    /// Version group ID, used to specify transaction type and validate its components.
    ///
    /// Size\[bytes\]: 4
    n_version_group_id: Option<u32>,
    /// Consensus branch ID, used to identify the network upgrade that the transaction is valid for.
    ///
    /// Size\[bytes\]: 4
    consensus_branch_id: u32,
    /// List of transparent inputs in a transaction.
    ///
    /// Size\[bytes\]: Vec<40+CompactSize>
    transparent_inputs: Vec<TxIn>,
    /// List of transparent outputs in a transaction.
    ///
    /// Size\[bytes\]: Vec<8+CompactSize>
    transparent_outputs: Vec<TxOut>,
    // NLockTime \[IGNORED\] - Size\[bytes\]: 4
    // NExpiryHeight \[IGNORED\] - Size\[bytes\]: 4
    // ValueBalanceSapling - Size\[bytes\]: 8
    /// Value balance for the Sapling pool (v4/v5). None if not present.
    value_balance_sapling: Option<i64>,
    /// List of shielded spends from the Sapling pool
    ///
    /// Size\[bytes\]: Vec<384>
    shielded_spends: Vec<Spend>,
    /// List of shielded outputs from the Sapling pool
    ///
    /// Size\[bytes\]: Vec<948>
    shielded_outputs: Vec<Output>,
    /// List of JoinSplit descriptions in a transaction, no longer supported.
    ///
    /// Size\[bytes\]: Vec<1602-1698>
    #[allow(dead_code)]
    join_splits: Vec<JoinSplit>,
    /// joinSplitPubKey \[IGNORED\] - Size\[bytes\]: 32
    /// joinSplitSig \[IGNORED\] - Size\[bytes\]: 64
    /// bindingSigSapling \[IGNORED\] - Size\[bytes\]: 64
    /// List of Orchard actions.
    ///
    /// Size\[bytes\]: Vec<820>
    orchard_actions: Vec<Action>,
    /// ValueBalanceOrchard - Size\[bytes\]: 8
    /// Value balance for the Orchard pool (v5 only). None if not present.
    value_balance_orchard: Option<i64>,
    /// AnchorOrchard - Size\[bytes\]: 32
    /// In non-coinbase transactions, this is the anchor (authDataRoot) of a prior block's Orchard note commitment tree.
    /// In the coinbase transaction, this commits to the final Orchard tree state for the current block — i.e., it *is* the block's authDataRoot.
    /// Present in v5 transactions only, if any Orchard actions exist in the block.
    anchor_orchard: Option<Vec<u8>>,
}

impl TransactionData {
    /// Parses a v1 transaction.
    ///
    /// A v1 transaction contains the following fields:
    ///
    /// - header: u32
    /// - tx_in_count: usize
    /// - tx_in: tx_in
    /// - tx_out_count: usize
    /// - tx_out: tx_out
    /// - lock_time: u32
    pub(crate) fn parse_v1(data: &[u8], version: u32) -> Result<(&[u8], Self), ParseError> {
        let mut cursor = Cursor::new(data);

        let (remaining_data, transparent_inputs, transparent_outputs) =
            parse_transparent(&data[cursor.position() as usize..])?;
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);

        // let lock_time = read_u32(&mut cursor, "Error reading TransactionData::lock_time")?;
        skip_bytes(&mut cursor, 4, "Error skipping TransactionData::nLockTime")?;

        Ok((
            &data[cursor.position() as usize..],
            TransactionData {
                f_overwintered: true,
                version,
                consensus_branch_id: 0,
                transparent_inputs,
                transparent_outputs,
                // lock_time: Some(lock_time),
                n_version_group_id: None,
                value_balance_sapling: None,
                shielded_spends: Vec::new(),
                shielded_outputs: Vec::new(),
                join_splits: Vec::new(),
                orchard_actions: Vec::new(),
                value_balance_orchard: None,
                anchor_orchard: None,
            },
        ))
    }

    /// Parses a v2 transaction.
    ///
    /// A v2 transaction contains the following fields:
    ///
    /// - header: u32
    /// - tx_in_count: usize
    /// - tx_in: tx_in
    /// - tx_out_count: usize
    /// - tx_out: tx_out
    /// - lock_time: u32
    /// - nJoinSplit: compactSize <- New
    /// - vJoinSplit: JSDescriptionBCTV14\[nJoinSplit\] <- New
    /// - joinSplitPubKey: byte\[32\] <- New
    /// - joinSplitSig: byte\[64\] <- New
    pub(crate) fn parse_v2(data: &[u8], version: u32) -> Result<(&[u8], Self), ParseError> {
        let mut cursor = Cursor::new(data);

        let (remaining_data, transparent_inputs, transparent_outputs) =
            parse_transparent(&data[cursor.position() as usize..])?;
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);

        skip_bytes(&mut cursor, 4, "Error skipping TransactionData::nLockTime")?;

        let join_split_count = CompactSize::read(&mut cursor)?;
        let mut join_splits = Vec::with_capacity(join_split_count as usize);
        for _ in 0..join_split_count {
            let (remaining_data, join_split) =
                JoinSplit::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
            join_splits.push(join_split);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }

        if join_split_count > 0 {
            skip_bytes(
                &mut cursor,
                32,
                "Error skipping TransactionData::joinSplitPubKey",
            )?;
            skip_bytes(
                &mut cursor,
                64,
                "could not skip TransactionData::joinSplitSig",
            )?;
        }

        Ok((
            &data[cursor.position() as usize..],
            TransactionData {
                f_overwintered: true,
                version,
                consensus_branch_id: 0,
                transparent_inputs,
                transparent_outputs,
                join_splits,
                n_version_group_id: None,
                value_balance_sapling: None,
                shielded_spends: Vec::new(),
                shielded_outputs: Vec::new(),
                orchard_actions: Vec::new(),
                value_balance_orchard: None,
                anchor_orchard: None,
            },
        ))
    }

    /// Parses a v3 transaction.
    ///
    /// A v3 transaction contains the following fields:
    ///
    /// - header: u32
    /// - nVersionGroupId: u32 = 0x03C48270 <- New
    /// - tx_in_count: usize
    /// - tx_in: tx_in
    /// - tx_out_count: usize
    /// - tx_out: tx_out
    /// - lock_time: u32
    /// - nExpiryHeight: u32 <- New
    /// - nJoinSplit: compactSize
    /// - vJoinSplit: JSDescriptionBCTV14\[nJoinSplit\]
    /// - joinSplitPubKey: byte\[32\]
    /// - joinSplitSig: byte\[64\]
    pub(crate) fn parse_v3(
        data: &[u8],
        version: u32,
        n_version_group_id: u32,
    ) -> Result<(&[u8], Self), ParseError> {
        if n_version_group_id != 0x03C48270 {
            return Err(ParseError::InvalidData(
                "n_version_group_id must be 0x03C48270".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        let (remaining_data, transparent_inputs, transparent_outputs) =
            parse_transparent(&data[cursor.position() as usize..])?;
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);

        skip_bytes(&mut cursor, 4, "Error skipping TransactionData::nLockTime")?;
        skip_bytes(
            &mut cursor,
            4,
            "Error skipping TransactionData::nExpiryHeight",
        )?;

        let join_split_count = CompactSize::read(&mut cursor)?;
        let mut join_splits = Vec::with_capacity(join_split_count as usize);
        for _ in 0..join_split_count {
            let (remaining_data, join_split) =
                JoinSplit::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
            join_splits.push(join_split);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }

        if join_split_count > 0 {
            skip_bytes(
                &mut cursor,
                32,
                "Error skipping TransactionData::joinSplitPubKey",
            )?;
            skip_bytes(
                &mut cursor,
                64,
                "could not skip TransactionData::joinSplitSig",
            )?;
        }
        Ok((
            &data[cursor.position() as usize..],
            TransactionData {
                f_overwintered: true,
                version,
                consensus_branch_id: 0,
                transparent_inputs,
                transparent_outputs,
                join_splits,
                n_version_group_id: None,
                value_balance_sapling: None,
                shielded_spends: Vec::new(),
                shielded_outputs: Vec::new(),
                orchard_actions: Vec::new(),
                value_balance_orchard: None,
                anchor_orchard: None,
            },
        ))
    }

    fn parse_v4(
        data: &[u8],
        version: u32,
        n_version_group_id: u32,
    ) -> Result<(&[u8], Self), ParseError> {
        if n_version_group_id != 0x892F2085 {
            return Err(ParseError::InvalidData(format!(
                "version group ID {n_version_group_id:x} must be 0x892F2085 for v4 transactions"
            )));
        }
        let mut cursor = Cursor::new(data);

        let (remaining_data, transparent_inputs, transparent_outputs) =
            parse_transparent(&data[cursor.position() as usize..])?;
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);

        skip_bytes(&mut cursor, 4, "Error skipping TransactionData::nLockTime")?;
        skip_bytes(
            &mut cursor,
            4,
            "Error skipping TransactionData::nExpiryHeight",
        )?;
        let value_balance_sapling = Some(read_i64(
            &mut cursor,
            "Error reading TransactionData::valueBalanceSapling",
        )?);

        let spend_count = CompactSize::read(&mut cursor)?;
        let mut shielded_spends = Vec::with_capacity(spend_count as usize);
        for _ in 0..spend_count {
            let (remaining_data, spend) =
                Spend::parse_from_slice(&data[cursor.position() as usize..], None, Some(4))?;
            shielded_spends.push(spend);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }
        let output_count = CompactSize::read(&mut cursor)?;
        let mut shielded_outputs = Vec::with_capacity(output_count as usize);
        for _ in 0..output_count {
            let (remaining_data, output) =
                Output::parse_from_slice(&data[cursor.position() as usize..], None, Some(4))?;
            shielded_outputs.push(output);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }
        let join_split_count = CompactSize::read(&mut cursor)?;
        let mut join_splits = Vec::with_capacity(join_split_count as usize);
        for _ in 0..join_split_count {
            let (remaining_data, join_split) =
                JoinSplit::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
            join_splits.push(join_split);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }

        if join_split_count > 0 {
            skip_bytes(
                &mut cursor,
                32,
                "Error skipping TransactionData::joinSplitPubKey",
            )?;
            skip_bytes(
                &mut cursor,
                64,
                "could not skip TransactionData::joinSplitSig",
            )?;
        }
        if spend_count + output_count > 0 {
            skip_bytes(
                &mut cursor,
                64,
                "Error skipping TransactionData::bindingSigSapling",
            )?;
        }

        Ok((
            &data[cursor.position() as usize..],
            TransactionData {
                f_overwintered: true,
                version,
                n_version_group_id: Some(n_version_group_id),
                consensus_branch_id: 0,
                transparent_inputs,
                transparent_outputs,
                value_balance_sapling,
                shielded_spends,
                shielded_outputs,
                join_splits,
                orchard_actions: Vec::new(),
                value_balance_orchard: None,
                anchor_orchard: None,
            },
        ))
    }

    fn parse_v5(
        data: &[u8],
        version: u32,
        n_version_group_id: u32,
    ) -> Result<(&[u8], Self), ParseError> {
        if n_version_group_id != 0x26A7270A {
            return Err(ParseError::InvalidData(format!(
                "version group ID {n_version_group_id:x} must be 0x892F2085 for v5 transactions"
            )));
        }
        let mut cursor = Cursor::new(data);

        let consensus_branch_id = read_u32(
            &mut cursor,
            "Error reading TransactionData::ConsensusBranchId",
        )?;

        skip_bytes(&mut cursor, 4, "Error skipping TransactionData::nLockTime")?;
        skip_bytes(
            &mut cursor,
            4,
            "Error skipping TransactionData::nExpiryHeight",
        )?;

        let (remaining_data, transparent_inputs, transparent_outputs) =
            parse_transparent(&data[cursor.position() as usize..])?;
        cursor.set_position(data.len() as u64 - remaining_data.len() as u64);

        let spend_count = CompactSize::read(&mut cursor)?;
        if spend_count >= (1 << 16) {
            return Err(ParseError::InvalidData(format!(
                "spendCount ({spend_count}) must be less than 2^16"
            )));
        }
        let mut shielded_spends = Vec::with_capacity(spend_count as usize);
        for _ in 0..spend_count {
            let (remaining_data, spend) =
                Spend::parse_from_slice(&data[cursor.position() as usize..], None, Some(5))?;
            shielded_spends.push(spend);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }
        let output_count = CompactSize::read(&mut cursor)?;
        if output_count >= (1 << 16) {
            return Err(ParseError::InvalidData(format!(
                "outputCount ({output_count}) must be less than 2^16"
            )));
        }
        let mut shielded_outputs = Vec::with_capacity(output_count as usize);
        for _ in 0..output_count {
            let (remaining_data, output) =
                Output::parse_from_slice(&data[cursor.position() as usize..], None, Some(5))?;
            shielded_outputs.push(output);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }

        let value_balance_sapling = if spend_count + output_count > 0 {
            Some(read_i64(
                &mut cursor,
                "Error reading TransactionData::valueBalanceSapling",
            )?)
        } else {
            None
        };
        if spend_count > 0 {
            skip_bytes(
                &mut cursor,
                32,
                "Error skipping TransactionData::anchorSapling",
            )?;
            skip_bytes(
                &mut cursor,
                (192 * spend_count) as usize,
                "Error skipping TransactionData::vSpendProofsSapling",
            )?;
            skip_bytes(
                &mut cursor,
                (64 * spend_count) as usize,
                "Error skipping TransactionData::vSpendAuthSigsSapling",
            )?;
        }
        if output_count > 0 {
            skip_bytes(
                &mut cursor,
                (192 * output_count) as usize,
                "Error skipping TransactionData::vOutputProofsSapling",
            )?;
        }
        if spend_count + output_count > 0 {
            skip_bytes(
                &mut cursor,
                64,
                "Error skipping TransactionData::bindingSigSapling",
            )?;
        }

        let actions_count = CompactSize::read(&mut cursor)?;
        if actions_count >= (1 << 16) {
            return Err(ParseError::InvalidData(format!(
                "actionsCount ({actions_count}) must be less than 2^16"
            )));
        }
        let mut orchard_actions = Vec::with_capacity(actions_count as usize);
        for _ in 0..actions_count {
            let (remaining_data, action) =
                Action::parse_from_slice(&data[cursor.position() as usize..], None, None)?;
            orchard_actions.push(action);
            cursor.set_position(data.len() as u64 - remaining_data.len() as u64);
        }

        let mut value_balance_orchard = None;
        let mut anchor_orchard = None;
        if actions_count > 0 {
            skip_bytes(
                &mut cursor,
                1,
                "Error skipping TransactionData::flagsOrchard",
            )?;
            value_balance_orchard = Some(read_i64(
                &mut cursor,
                "Error reading TransactionData::valueBalanceOrchard",
            )?);
            anchor_orchard = Some(read_bytes(
                &mut cursor,
                32,
                "Error reading TransactionData::anchorOrchard",
            )?);
            let proofs_count = CompactSize::read(&mut cursor)?;
            skip_bytes(
                &mut cursor,
                proofs_count as usize,
                "Error skipping TransactionData::proofsOrchard",
            )?;
            skip_bytes(
                &mut cursor,
                (64 * actions_count) as usize,
                "Error skipping TransactionData::vSpendAuthSigsOrchard",
            )?;
            skip_bytes(
                &mut cursor,
                64,
                "Error skipping TransactionData::bindingSigOrchard",
            )?;
        }

        Ok((
            &data[cursor.position() as usize..],
            TransactionData {
                f_overwintered: true,
                version,
                n_version_group_id: Some(n_version_group_id),
                consensus_branch_id,
                transparent_inputs,
                transparent_outputs,
                value_balance_sapling,
                shielded_spends,
                shielded_outputs,
                join_splits: Vec::new(),
                orchard_actions,
                value_balance_orchard,
                anchor_orchard,
            },
        ))
    }
}

/// Zingo-Indexer struct for a full zcash transaction.
#[derive(Debug, Clone)]
pub struct FullTransaction {
    /// Full transaction data.
    raw_transaction: TransactionData,

    /// Raw transaction bytes.
    raw_bytes: Vec<u8>,

    /// Transaction Id, fetched using get_block JsonRPC with verbose = 1.
    tx_id: Vec<u8>,
}

impl ParseFromSlice for FullTransaction {
    fn parse_from_slice(
        data: &[u8],
        txid: Option<Vec<Vec<u8>>>,
        tx_version: Option<u32>,
    ) -> Result<(&[u8], Self), ParseError> {
        let txid = txid.ok_or_else(|| {
            ParseError::InvalidData(
                "txid must be used for FullTransaction::parse_from_slice".to_string(),
            )
        })?;
        if tx_version.is_some() {
            return Err(ParseError::InvalidData(
                "tx_version must be None for FullTransaction::parse_from_slice".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);

        let header = read_u32(&mut cursor, "Error reading FullTransaction::header")?;
        let f_overwintered = (header >> 31) == 1;

        let version = header & 0x7FFFFFFF;

        match version {
            1 | 2 => {
                if f_overwintered {
                    return Err(ParseError::InvalidData(
                        "fOverwintered must be unset for tx versions 1 and 2".to_string(),
                    ));
                }
            }
            3..=5 => {
                if !f_overwintered {
                    return Err(ParseError::InvalidData(
                        "fOverwintered must be set for tx versions 3 and above".to_string(),
                    ));
                }
            }
            _ => {
                return Err(ParseError::InvalidData(format!(
                    "Unsupported tx version {version}"
                )))
            }
        }

        let n_version_group_id: Option<u32> = match version {
            3..=5 => Some(read_u32(
                &mut cursor,
                "Error reading FullTransaction::n_version_group_id",
            )?),
            _ => None,
        };

        let (remaining_data, transaction_data) = match version {
            1 => TransactionData::parse_v1(&data[cursor.position() as usize..], version)?,
            2 => TransactionData::parse_v2(&data[cursor.position() as usize..], version)?,
            3 => TransactionData::parse_v3(
                &data[cursor.position() as usize..],
                version,
                n_version_group_id.unwrap(), // This won't fail, because of the above match
            )?,
            4 => TransactionData::parse_v4(
                &data[cursor.position() as usize..],
                version,
                n_version_group_id.unwrap(), // This won't fail, because of the above match
            )?,
            5 => TransactionData::parse_v5(
                &data[cursor.position() as usize..],
                version,
                n_version_group_id.unwrap(), // This won't fail, because of the above match
            )?,

            _ => {
                return Err(ParseError::InvalidData(format!(
                    "Unsupported tx version {version}"
                )))
            }
        };

        let full_transaction = FullTransaction {
            raw_transaction: transaction_data,
            raw_bytes: data[..(data.len() - remaining_data.len())].to_vec(),
            tx_id: txid[0].clone(),
        };

        Ok((remaining_data, full_transaction))
    }
}

impl FullTransaction {
    /// Returns overwintered bool
    pub fn f_overwintered(&self) -> bool {
        self.raw_transaction.f_overwintered
    }

    /// Returns the transaction version.
    pub fn version(&self) -> u32 {
        self.raw_transaction.version
    }

    /// Returns the transaction version group id.
    pub fn n_version_group_id(&self) -> Option<u32> {
        self.raw_transaction.n_version_group_id
    }

    /// returns the consensus branch id of the transaction.
    pub fn consensus_branch_id(&self) -> u32 {
        self.raw_transaction.consensus_branch_id
    }

    /// Returns a vec of transparent inputs: (prev_txid, prev_index, script_sig).
    pub fn transparent_inputs(&self) -> Vec<(Vec<u8>, u32, Vec<u8>)> {
        self.raw_transaction
            .transparent_inputs
            .iter()
            .map(|input| input.clone().into_inner())
            .collect()
    }

    /// Returns a vec of transparent outputs: (value, script_hash).
    pub fn transparent_outputs(&self) -> Vec<(u64, Vec<u8>)> {
        self.raw_transaction
            .transparent_outputs
            .iter()
            .map(|output| output.clone().into_inner())
            .collect()
    }

    /// Returns sapling and orchard value balances for the transaction.
    ///
    /// Returned as (Option\<valueBalanceSapling\>, Option\<valueBalanceOrchard\>).
    pub fn value_balances(&self) -> (Option<i64>, Option<i64>) {
        (
            self.raw_transaction.value_balance_sapling,
            self.raw_transaction.value_balance_orchard,
        )
    }

    /// Returns a vec of sapling nullifiers for the transaction.
    pub fn shielded_spends(&self) -> Vec<Vec<u8>> {
        self.raw_transaction
            .shielded_spends
            .iter()
            .map(|input| input.clone().into_inner())
            .collect()
    }

    /// Returns a vec of sapling outputs (cmu, ephemeral_key, enc_ciphertext) for the transaction.
    pub fn shielded_outputs(&self) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        self.raw_transaction
            .shielded_outputs
            .iter()
            .map(|input| input.clone().into_parts())
            .collect()
    }

    /// Returns None as joinsplits are not supported in Zaino.
    pub fn join_splits(&self) -> Option<()> {
        None
    }

    /// Returns a vec of orchard actions (nullifier, cmx, ephemeral_key, enc_ciphertext) for the transaction.
    #[allow(clippy::complexity)]
    pub fn orchard_actions(&self) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> {
        self.raw_transaction
            .orchard_actions
            .iter()
            .map(|input| input.clone().into_parts())
            .collect()
    }

    /// Returns the orchard anchor of the transaction.
    ///
    /// If this is the Coinbase transaction then this returns the AuthDataRoot of the block.
    pub fn anchor_orchard(&self) -> Option<Vec<u8>> {
        self.raw_transaction.anchor_orchard.clone()
    }

    /// Returns the transaction as raw bytes.
    pub fn raw_bytes(&self) -> Vec<u8> {
        self.raw_bytes.clone()
    }

    /// returns the TxId of the transaction.
    pub fn tx_id(&self) -> Vec<u8> {
        self.tx_id.clone()
    }

    /// Converts a zcash full transaction into a compact transaction.
    pub fn to_compact(self, index: u64) -> Result<CompactTx, ParseError> {
        let hash = self.tx_id;

        // NOTE: LightWalletD currently does not return a fee and is not currently priority here. Please open an Issue or PR at the Zingo-Indexer github (https://github.com/zingolabs/zingo-indexer) if you require this functionality.
        let fee = 0;

        let spends = self
            .raw_transaction
            .shielded_spends
            .iter()
            .map(|spend| CompactSaplingSpend {
                nf: spend.nullifier.clone(),
            })
            .collect();

        let outputs = self
            .raw_transaction
            .shielded_outputs
            .iter()
            .map(|output| CompactSaplingOutput {
                cmu: output.cmu.clone(),
                ephemeral_key: output.ephemeral_key.clone(),
                ciphertext: output.enc_ciphertext[..52].to_vec(),
            })
            .collect();

        let actions = self
            .raw_transaction
            .orchard_actions
            .iter()
            .map(|action| CompactOrchardAction {
                nullifier: action.nullifier.clone(),
                cmx: action.cmx.clone(),
                ephemeral_key: action.ephemeral_key.clone(),
                ciphertext: action.enc_ciphertext[..52].to_vec(),
            })
            .collect();

        Ok(CompactTx {
            index,
            hash,
            fee,
            spends,
            outputs,
            actions,
        })
    }

    /// Returns true if the transaction contains either sapling spends or outputs.
    pub(crate) fn has_shielded_elements(&self) -> bool {
        !self.raw_transaction.shielded_spends.is_empty()
            || !self.raw_transaction.shielded_outputs.is_empty()
            || !self.raw_transaction.orchard_actions.is_empty()
    }
}

/// Consensus validation error types
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum ConsensusError {
    #[error("Invalid version: {version}, must be >= 1")]
    InvalidVersion { version: u32 },

    #[error("Invalid version group ID: expected {expected:#x}, got {actual:#x}")]
    InvalidVersionGroupId { expected: u32, actual: u32 },

    #[error("Overwintered flag not set for version {version}")]
    OverwinteredFlagNotSet { version: u32 },

    #[error("No source of funds for version {version} transaction")]
    NoSourceOfFunds { version: u32 },

    #[error("Transaction too large: {size} bytes, max {max_size}")]
    TransactionTooLarge { size: usize, max_size: usize },
}

// This trait is used, there's a false positive dead code detection for some reason
#[allow(dead_code)]
/// Enhanced TransactionField trait with default order validation and counter management
trait TransactionField {
    type Value;
    const SIZE: Option<usize>;
    const EXPECTED_ORDER: u8;

    /// Core parsing logic - implement this
    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError>;

    /// Field-specific validation - implement this if needed
    fn validate_field(_value: &Self::Value) -> Result<(), ConsensusError> {
        Ok(()) // Default: no validation
    }

    /// Complete read with order validation and counter management - DON'T override this
    fn read_and_validate(
        cursor: &mut Cursor<&[u8]>,
        counter: &mut OrderCounter,
    ) -> Result<Self::Value, ParseError> {
        // Order validation
        if counter.current != Self::EXPECTED_ORDER {
            return Err(ParseError::InvalidParseOrder {
                field: std::any::type_name::<Self>(),
                expected_order: Self::EXPECTED_ORDER,
                actual_order: counter.current,
            });
        }

        // Optional size validation (for debugging)
        let start_pos = cursor.position();

        // Parse the field
        let value = Self::read_from_cursor(cursor)?;

        // Validate expected size advancement (for fixed-size fields)
        if let Some(expected_size) = Self::SIZE {
            let actual_advancement = cursor.position() - start_pos;
            if actual_advancement != expected_size as u64 {
                return Err(ParseError::UnexpectedFieldSize {
                    field: std::any::type_name::<Self>(),
                    expected: expected_size,
                    actual: actual_advancement as usize,
                });
            }
        }

        // Field-specific validation
        Self::validate_field(&value)?;

        // Advance counter
        counter.advance();

        Ok(value)
    }
}

// This trait is used, there's a false positive dead code detection for some reason
#[allow(dead_code)]
/// TransactionReader trait for parsing, validating, and converting transactions
trait TransactionReader: Sized {
    type Transaction;
    type Error;

    /// Read transaction from cursor
    fn read(cursor: &mut Cursor<&[u8]>) -> Result<Self, Self::Error>;

    /// Validate consensus rules
    fn validate(&self) -> Result<(), Self::Error>;

    /// Transform to clean transaction struct
    fn to_transaction(self) -> Result<Self::Transaction, Self::Error>;

    /// One-shot parse, validate, and convert
    fn parse_and_validate(cursor: &mut Cursor<&[u8]>) -> Result<Self::Transaction, Self::Error> {
        let reader = Self::read(cursor)?;
        reader.validate()?;
        reader.to_transaction()
    }
}

/// Helper struct for order management
struct OrderCounter {
    current: u8,
}

impl OrderCounter {
    fn new() -> Self {
        Self { current: 0 }
    }

    fn advance(&mut self) {
        self.current += 1;
    }
}

// ===== Field Type Implementations =====

/// Transaction header field (4 bytes: f_overwintered + version)
struct Header;

impl TransactionField for Header {
    type Value = u32;
    const SIZE: Option<usize> = Some(4);
    const EXPECTED_ORDER: u8 = 0;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        read_u32(cursor, "Error reading header")
    }

    fn validate_field(value: &Self::Value) -> Result<(), ConsensusError> {
        let version = value & 0x7FFFFFFF;
        if version < 1 {
            return Err(ConsensusError::InvalidVersion { version });
        }
        Ok(())
    }
}

/// Version group ID field (4 bytes)
struct VersionGroupId;

impl TransactionField for VersionGroupId {
    type Value = u32;
    const SIZE: Option<usize> = Some(4);
    const EXPECTED_ORDER: u8 = 1;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        read_u32(cursor, "Error reading version group ID")
    }
}

/// Transparent inputs field (CompactSize count + variable data)
struct TransparentInputs;

impl TransactionField for TransparentInputs {
    type Value = Vec<TxIn>;
    const SIZE: Option<usize> = None; // Variable size
    const EXPECTED_ORDER: u8 = 2;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        let count = CompactSize::read(&mut *cursor)?;
        let mut inputs = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (remaining_data, tx_in) = TxIn::parse_from_slice(
                &cursor.get_ref()[(cursor.position() as usize)..],
                None,
                None,
            )?;
            inputs.push(tx_in);
            cursor.set_position(cursor.get_ref().len() as u64 - remaining_data.len() as u64);
        }

        Ok(inputs)
    }
}

/// Transparent outputs field (CompactSize count + variable data)
struct TransparentOutputs;

impl TransactionField for TransparentOutputs {
    type Value = Vec<TxOut>;
    const SIZE: Option<usize> = None; // Variable size
    const EXPECTED_ORDER: u8 = 3;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        let count = CompactSize::read(&mut *cursor)?;
        let mut outputs = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (remaining_data, tx_out) = TxOut::parse_from_slice(
                &cursor.get_ref()[(cursor.position() as usize)..],
                None,
                None,
            )?;
            outputs.push(tx_out);
            cursor.set_position(cursor.get_ref().len() as u64 - remaining_data.len() as u64);
        }

        Ok(outputs)
    }
}

/// Lock time field (4 bytes) - read but not used in final struct
struct LockTime;

impl TransactionField for LockTime {
    type Value = u32;
    const SIZE: Option<usize> = Some(4);
    const EXPECTED_ORDER: u8 = 4;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        read_u32(cursor, "Error reading lock time")
    }
}

/// Expiry height field (4 bytes) - read but not used in final struct
struct ExpiryHeight;

impl TransactionField for ExpiryHeight {
    type Value = u32;
    const SIZE: Option<usize> = Some(4);
    const EXPECTED_ORDER: u8 = 5;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        read_u32(cursor, "Error reading expiry height")
    }
}

/// Value balance sapling field (8 bytes)
struct ValueBalanceSapling;

impl TransactionField for ValueBalanceSapling {
    type Value = i64;
    const SIZE: Option<usize> = Some(8);
    const EXPECTED_ORDER: u8 = 6;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        read_i64(cursor, "Error reading value balance sapling")
    }
}

/// Shielded spends field (CompactSize count + variable data)
struct ShieldedSpends;

impl TransactionField for ShieldedSpends {
    type Value = Vec<Spend>;
    const SIZE: Option<usize> = None; // Variable size
    const EXPECTED_ORDER: u8 = 7;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        let count = CompactSize::read(&mut *cursor)?;
        let mut spends = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (remaining_data, spend) = Spend::parse_from_slice(
                &cursor.get_ref()[(cursor.position() as usize)..],
                None,
                Some(4), // V4 transaction
            )?;
            spends.push(spend);
            cursor.set_position(cursor.get_ref().len() as u64 - remaining_data.len() as u64);
        }

        Ok(spends)
    }
}

/// Shielded outputs field (CompactSize count + variable data)
struct ShieldedOutputs;

impl TransactionField for ShieldedOutputs {
    type Value = Vec<Output>;
    const SIZE: Option<usize> = None; // Variable size
    const EXPECTED_ORDER: u8 = 8;

    fn read_from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<Self::Value, ParseError> {
        let count = CompactSize::read(&mut *cursor)?;
        let mut outputs = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (remaining_data, output) = Output::parse_from_slice(
                &cursor.get_ref()[(cursor.position() as usize)..],
                None,
                Some(4), // V4 transaction
            )?;
            outputs.push(output);
            cursor.set_position(cursor.get_ref().len() as u64 - remaining_data.len() as u64);
        }

        Ok(outputs)
    }
}

// ===== TransactionV4Reader Implementation =====

/// Transaction V4 Reader - contains all parsed fields including "skip" fields
pub struct TransactionV4Reader {
    // All fields - including the ones we'll "skip" in final struct
    header: u32,
    version_group_id: u32,
    transparent_inputs: Vec<TxIn>,
    transparent_outputs: Vec<TxOut>,
    _lock_time: u32,     // Read but not used in final struct
    _expiry_height: u32, // Read but not used in final struct
    value_balance_sapling: i64,
    shielded_spends: Vec<Spend>,
    shielded_outputs: Vec<Output>,
}

impl TransactionV4Reader {
    // ===== Useful helper methods =====

    /// Get the transaction version from header
    pub fn get_version(&self) -> u32 {
        self.header & 0x7FFFFFFF
    }

    /// Check if transaction is overwintered
    pub fn is_overwintered(&self) -> bool {
        (self.header >> 31) == 1
    }

    /// Get effective version (TODO: implement consensus-based logic)
    pub fn get_effective_version(&self) -> u32 {
        // TODO: Implement effective version logic based on consensus rules
        // This might involve network upgrade logic, activation heights, etc.
        self.get_version()
    }

    /// Check if transaction has transparent inputs
    pub fn has_transparent_inputs(&self) -> bool {
        !self.transparent_inputs.is_empty()
    }

    /// Check if transaction has transparent outputs
    pub fn has_transparent_outputs(&self) -> bool {
        !self.transparent_outputs.is_empty()
    }

    /// Check if transaction has any shielded elements
    pub fn has_shielded_elements(&self) -> bool {
        !self.shielded_spends.is_empty() || !self.shielded_outputs.is_empty()
    }
}

impl TransactionReader for TransactionV4Reader {
    type Transaction = TransactionV4;
    type Error = ParseError;

    fn read(cursor: &mut Cursor<&[u8]>) -> Result<Self, Self::Error> {
        let mut counter = OrderCounter::new();

        Ok(Self {
            header: Header::read_and_validate(cursor, &mut counter)?,
            version_group_id: VersionGroupId::read_and_validate(cursor, &mut counter)?,
            transparent_inputs: TransparentInputs::read_and_validate(cursor, &mut counter)?,
            transparent_outputs: TransparentOutputs::read_and_validate(cursor, &mut counter)?,

            // Read these fields but we'll skip them in final struct
            _lock_time: LockTime::read_and_validate(cursor, &mut counter)?,
            _expiry_height: ExpiryHeight::read_and_validate(cursor, &mut counter)?,

            value_balance_sapling: ValueBalanceSapling::read_and_validate(cursor, &mut counter)?,
            shielded_spends: ShieldedSpends::read_and_validate(cursor, &mut counter)?,
            shielded_outputs: ShieldedOutputs::read_and_validate(cursor, &mut counter)?,
        })
    }

    fn validate(&self) -> Result<(), Self::Error> {
        // Transaction-level consensus validation using helper methods
        self.validate_version_group_id()?;
        self.validate_overwintered_flags()?;
        self.validate_input_output_rules()?;
        self.validate_transaction_size()?;
        Ok(())
    }

    fn to_transaction(self) -> Result<Self::Transaction, Self::Error> {
        // Skip lock_time and expiry_height - they're not in the final struct
        Ok(TransactionV4 {
            header: self.header,
            version_group_id: self.version_group_id,
            transparent_inputs: self.transparent_inputs,
            transparent_outputs: self.transparent_outputs,
            // lock_time and expiry_height intentionally omitted
            value_balance_sapling: self.value_balance_sapling,
            shielded_spends: self.shielded_spends,
            shielded_outputs: self.shielded_outputs,
        })
    }
}

// ===== Transaction-level consensus validation methods =====
impl TransactionV4Reader {
    fn validate_version_group_id(&self) -> Result<(), ParseError> {
        if self.version_group_id != 0x892F2085 {
            return Err(ParseError::ConsensusError(
                ConsensusError::InvalidVersionGroupId {
                    expected: 0x892F2085,
                    actual: self.version_group_id,
                },
            ));
        }
        Ok(())
    }

    fn validate_overwintered_flags(&self) -> Result<(), ParseError> {
        let version = self.get_version();
        let is_overwintered = self.is_overwintered();

        if version >= 3 && !is_overwintered {
            return Err(ParseError::ConsensusError(
                ConsensusError::OverwinteredFlagNotSet { version },
            ));
        }

        Ok(())
    }

    fn validate_input_output_rules(&self) -> Result<(), ParseError> {
        let version = self.get_version();
        if version == 1 && !self.has_transparent_inputs() {
            return Err(ParseError::ConsensusError(
                ConsensusError::NoSourceOfFunds { version },
            ));
        }
        Ok(())
    }

    fn validate_transaction_size(&self) -> Result<(), ParseError> {
        // TODO: Calculate actual transaction size and validate against limits
        // For now, this is a placeholder
        Ok(())
    }
}

// ===== Final Clean Transaction Structs =====

/// Clean V4 Transaction struct with business logic helper methods
/// (excludes "skip" fields like lock_time, expiry_height that were read during parsing)
#[derive(Debug, Clone)]
pub struct TransactionV4 {
    header: u32,
    version_group_id: u32,
    transparent_inputs: Vec<TxIn>,
    transparent_outputs: Vec<TxOut>,
    value_balance_sapling: i64,
    shielded_spends: Vec<Spend>,
    shielded_outputs: Vec<Output>,
}

impl TransactionV4 {
    // ===== Business logic helper methods =====

    /// Get the transaction version from header
    pub fn get_version(&self) -> u32 {
        self.header & 0x7FFFFFFF
    }

    /// Check if transaction is overwintered
    pub fn is_overwintered(&self) -> bool {
        (self.header >> 31) == 1
    }

    /// Get effective version (TODO: implement consensus-based logic)
    pub fn get_effective_version(&self) -> u32 {
        // TODO: Implement effective version logic based on consensus rules
        // This might involve network upgrade logic, activation heights, etc.
        // For now, return the basic version
        self.get_version()
    }

    /// Get version group ID
    pub fn version_group_id(&self) -> u32 {
        self.version_group_id
    }

    /// Get transparent inputs
    pub fn transparent_inputs(&self) -> &[TxIn] {
        &self.transparent_inputs
    }

    /// Get transparent outputs
    pub fn transparent_outputs(&self) -> &[TxOut] {
        &self.transparent_outputs
    }

    /// Get shielded spends
    pub fn shielded_spends(&self) -> &[Spend] {
        &self.shielded_spends
    }

    /// Get shielded outputs
    pub fn shielded_outputs(&self) -> &[Output] {
        &self.shielded_outputs
    }

    /// Get sapling value balance
    pub fn value_balance_sapling(&self) -> i64 {
        self.value_balance_sapling
    }

    /// Check if transaction has transparent inputs
    pub fn has_transparent_inputs(&self) -> bool {
        !self.transparent_inputs.is_empty()
    }

    /// Check if transaction has transparent outputs
    pub fn has_transparent_outputs(&self) -> bool {
        !self.transparent_outputs.is_empty()
    }

    /// Check if transaction has any shielded elements
    pub fn has_shielded_elements(&self) -> bool {
        !self.shielded_spends.is_empty() || !self.shielded_outputs.is_empty()
    }

    /// Check if transaction is purely transparent
    pub fn is_transparent_only(&self) -> bool {
        !self.has_shielded_elements()
    }

    /// Check if transaction is purely shielded
    pub fn is_shielded_only(&self) -> bool {
        self.has_shielded_elements()
            && !self.has_transparent_inputs()
            && !self.has_transparent_outputs()
    }

    /// Get total number of transparent inputs
    pub fn transparent_input_count(&self) -> usize {
        self.transparent_inputs.len()
    }

    /// Get total number of transparent outputs
    pub fn transparent_output_count(&self) -> usize {
        self.transparent_outputs.len()
    }

    /// Get total number of shielded spends
    pub fn shielded_spend_count(&self) -> usize {
        self.shielded_spends.len()
    }

    /// Get total number of shielded outputs
    pub fn shielded_output_count(&self) -> usize {
        self.shielded_outputs.len()
    }
}

#[cfg(test)]
mod transaction_v4_tests {
    use super::*;
    use std::io::Cursor;

    /// Creates a minimal valid V4 transaction for testing
    ///
    /// Transaction structure:
    /// - Header: 4 bytes (version + overwintered flag)
    /// - Version Group ID: 4 bytes
    /// - Transparent inputs: CompactSize(0) + 0 inputs = 1 byte
    /// - Transparent outputs: CompactSize(0) + 0 outputs = 1 byte  
    /// - Lock time: 4 bytes
    /// - Expiry height: 4 bytes
    /// - Value balance sapling: 8 bytes
    /// - Shielded spends: CompactSize(0) + 0 spends = 1 byte
    /// - Shielded outputs: CompactSize(0) + 0 outputs = 1 byte
    ///
    /// Total: 28 bytes
    fn create_minimal_v4_transaction() -> Vec<u8> {
        let mut tx_data = Vec::new();

        // Header: version 4 with overwintered flag set (0x80000004)
        tx_data.extend_from_slice(&0x80000004u32.to_le_bytes());

        // Version Group ID for V4 (0x892F2085)
        tx_data.extend_from_slice(&0x892F2085u32.to_le_bytes());

        // Transparent inputs: count = 0
        tx_data.push(0x00); // CompactSize(0)

        // Transparent outputs: count = 0
        tx_data.push(0x00); // CompactSize(0)

        // Lock time: 4 bytes
        tx_data.extend_from_slice(&500000u32.to_le_bytes());

        // Expiry height: 4 bytes
        tx_data.extend_from_slice(&500100u32.to_le_bytes());

        // Value balance sapling: 8 bytes
        tx_data.extend_from_slice(&0i64.to_le_bytes());

        // Shielded spends: count = 0
        tx_data.push(0x00); // CompactSize(0)

        // Shielded outputs: count = 0
        tx_data.push(0x00); // CompactSize(0)

        tx_data
    }

    #[test]
    fn test_transaction_field_order_validation() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());
        let mut counter = OrderCounter::new();

        // Test that fields must be read in the correct order
        assert_eq!(counter.current, 0);

        // Reading header (order 0) should work
        let header = Header::read_and_validate(&mut cursor, &mut counter);
        assert!(header.is_ok());
        assert_eq!(counter.current, 1);

        // Try to read another header (wrong order) should fail
        let invalid_read = Header::read_and_validate(&mut cursor, &mut counter);
        assert!(invalid_read.is_err());

        // Reset cursor and counter to test proper sequence
        cursor.set_position(0);
        counter.current = 0;

        // Test complete valid sequence
        let _header = Header::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _version_group_id =
            VersionGroupId::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _transparent_inputs =
            TransparentInputs::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _transparent_outputs =
            TransparentOutputs::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _lock_time = LockTime::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _expiry_height = ExpiryHeight::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _value_balance =
            ValueBalanceSapling::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _shielded_spends =
            ShieldedSpends::read_and_validate(&mut cursor, &mut counter).unwrap();
        let _shielded_outputs =
            ShieldedOutputs::read_and_validate(&mut cursor, &mut counter).unwrap();

        assert_eq!(counter.current, 9); // All 9 fields read successfully
    }

    #[test]
    fn test_transaction_v4_reader_basic_parsing() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());

        // Test basic parsing
        let reader = TransactionV4Reader::read(&mut cursor);
        assert!(
            reader.is_ok(),
            "Failed to parse V4 transaction: {:?}",
            reader.err()
        );

        let reader = reader.unwrap();

        // Check header parsing
        assert_eq!(reader.get_version(), 4);
        assert!(reader.is_overwintered());
        assert_eq!(reader.get_effective_version(), 4);

        // Check empty counts
        assert!(!reader.has_transparent_inputs());
        assert!(!reader.has_transparent_outputs());
        assert!(!reader.has_shielded_elements());
    }

    #[test]
    fn test_transaction_v4_reader_validation() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());

        let reader = TransactionV4Reader::read(&mut cursor).unwrap();

        // Test consensus validation passes for minimal transaction
        let validation_result = reader.validate();
        assert!(
            validation_result.is_ok(),
            "Validation failed: {:?}",
            validation_result.err()
        );
    }

    #[test]
    fn test_transaction_v4_reader_invalid_version_group_id() {
        let mut tx_data = create_minimal_v4_transaction();

        // Change version group ID to invalid value (bytes 4-7)
        tx_data[4] = 0x00;
        tx_data[5] = 0x00;
        tx_data[6] = 0x00;
        tx_data[7] = 0x00;

        let mut cursor = Cursor::new(tx_data.as_slice());
        let reader = TransactionV4Reader::read(&mut cursor).unwrap();

        // Validation should fail for invalid version group ID
        let validation_result = reader.validate();
        assert!(validation_result.is_err());

        if let Err(ParseError::ConsensusError(ConsensusError::InvalidVersionGroupId {
            expected,
            actual,
        })) = validation_result
        {
            assert_eq!(expected, 0x892F2085);
            assert_eq!(actual, 0x00000000);
        } else {
            panic!("Expected InvalidVersionGroupId error, got: {validation_result:?}");
        }
    }

    #[test]
    fn test_transaction_v4_reader_to_clean_transaction() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());

        let reader = TransactionV4Reader::read(&mut cursor).unwrap();
        reader.validate().unwrap(); // Should pass validation

        // Convert to clean transaction struct
        let transaction = reader.to_transaction();
        assert!(
            transaction.is_ok(),
            "Failed to convert to transaction: {:?}",
            transaction.err()
        );

        let transaction = transaction.unwrap();

        // Test business logic methods
        assert_eq!(transaction.get_version(), 4);
        assert!(transaction.is_overwintered());
        assert_eq!(transaction.version_group_id(), 0x892F2085);
        assert_eq!(transaction.transparent_input_count(), 0);
        assert_eq!(transaction.transparent_output_count(), 0);
        assert_eq!(transaction.shielded_spend_count(), 0);
        assert_eq!(transaction.shielded_output_count(), 0);
        assert!(transaction.is_transparent_only()); // No shielded elements
        assert!(!transaction.is_shielded_only()); // Also no transparent elements
        assert_eq!(transaction.value_balance_sapling(), 0);
    }

    #[test]
    fn test_transaction_v4_reader_parse_and_validate_one_shot() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());

        // Test one-shot parse and validate
        let transaction = TransactionV4Reader::parse_and_validate(&mut cursor);
        assert!(
            transaction.is_ok(),
            "One-shot parsing failed: {:?}",
            transaction.err()
        );

        let transaction = transaction.unwrap();
        assert_eq!(transaction.get_version(), 4);
        assert!(transaction.is_overwintered());
    }

    #[test]
    fn test_header_field_validation() {
        // Test invalid version (version 0)
        let mut tx_data = create_minimal_v4_transaction();
        tx_data[0] = 0x00; // Set version to 0 (invalid)
        tx_data[1] = 0x00;
        tx_data[2] = 0x00;
        tx_data[3] = 0x80; // Keep overwintered flag

        let mut cursor = Cursor::new(tx_data.as_slice());
        let mut counter = OrderCounter::new();

        let header_result = Header::read_and_validate(&mut cursor, &mut counter);
        assert!(header_result.is_err());

        if let Err(ParseError::ConsensusError(ConsensusError::InvalidVersion { version })) =
            header_result
        {
            assert_eq!(version, 0);
        } else {
            panic!("Expected InvalidVersion error, got: {header_result:?}");
        }
    }

    #[test]
    fn test_overwintered_flag_validation() {
        // Test version 3 without overwintered flag (should fail validation)
        let mut tx_data = create_minimal_v4_transaction();
        tx_data[0] = 0x03; // Version 3
        tx_data[1] = 0x00;
        tx_data[2] = 0x00;
        tx_data[3] = 0x00; // No overwintered flag

        let mut cursor = Cursor::new(tx_data.as_slice());
        let reader = TransactionV4Reader::read(&mut cursor).unwrap();

        let validation_result = reader.validate();
        assert!(validation_result.is_err());

        if let Err(ParseError::ConsensusError(ConsensusError::OverwinteredFlagNotSet { version })) =
            validation_result
        {
            assert_eq!(version, 3);
        } else {
            panic!("Expected OverwinteredFlagNotSet error, got: {validation_result:?}");
        }
    }

    #[test]
    fn test_transaction_v4_helper_methods() {
        let tx_data = create_minimal_v4_transaction();
        let mut cursor = Cursor::new(tx_data.as_slice());

        let reader = TransactionV4Reader::read(&mut cursor).unwrap();

        // Test reader helper methods
        assert_eq!(reader.get_version(), 4);
        assert!(reader.is_overwintered());
        assert_eq!(reader.get_effective_version(), 4); // TODO will be enhanced later
        assert!(!reader.has_transparent_inputs());
        assert!(!reader.has_transparent_outputs());
        assert!(!reader.has_shielded_elements());

        // Convert to transaction and test its helper methods
        let transaction = reader.to_transaction().unwrap();

        assert_eq!(transaction.get_version(), 4);
        assert!(transaction.is_overwintered());
        assert_eq!(transaction.get_effective_version(), 4); // TODO will be enhanced later
        assert_eq!(transaction.version_group_id(), 0x892F2085);
        assert_eq!(transaction.transparent_inputs().len(), 0);
        assert_eq!(transaction.transparent_outputs().len(), 0);
        assert_eq!(transaction.shielded_spends().len(), 0);
        assert_eq!(transaction.shielded_outputs().len(), 0);
        assert_eq!(transaction.value_balance_sapling(), 0);
        assert!(!transaction.has_transparent_inputs());
        assert!(!transaction.has_transparent_outputs());
        assert!(!transaction.has_shielded_elements());
        assert!(transaction.is_transparent_only()); // No shielded elements
        assert!(!transaction.is_shielded_only()); // No elements at all
        assert_eq!(transaction.transparent_input_count(), 0);
        assert_eq!(transaction.transparent_output_count(), 0);
        assert_eq!(transaction.shielded_spend_count(), 0);
        assert_eq!(transaction.shielded_output_count(), 0);
    }

    #[test]
    fn test_field_size_validation() {
        // Create transaction data with wrong header size (should be 4 bytes)
        let mut short_tx_data = Vec::new();
        short_tx_data.extend_from_slice(&[0x04, 0x00, 0x80]); // Only 3 bytes instead of 4

        let mut cursor = Cursor::new(short_tx_data.as_slice());
        let mut counter = OrderCounter::new();

        // This should fail because Header expects exactly 4 bytes but cursor only has 3
        let header_result = Header::read_and_validate(&mut cursor, &mut counter);
        assert!(header_result.is_err());
    }
}
