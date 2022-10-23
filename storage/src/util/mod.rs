const MASK_DELTA: u32 = 0xa282ead8;

pub fn crc_mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
}

pub fn crc_unmask(crc_masked: u32) -> u32 {
    let rot = crc_masked.wrapping_sub(MASK_DELTA);
    (rot >> 17) | (rot << 15)
}
