//! Platform mapping for pairing.
//!
//! `biz.wa_user.platform` stores the **official** WhatsApp `PlatformType`
//! (0-25) as the single source of truth — the client sends the official value
//! directly in its `X-Platform` header, so there is no business-side mapping
//! table. This module derives the pairing-time `companion_platform_id` +
//! `companion_platform_display` from that official type.
//!
//! Wire semantics (see `wacore::companion_reg`):
//! - `companion_platform_id` is a single ASCII byte chosen from
//!   [`CompanionWebClientType`]. Android letters ('d'/'e'/'f') require
//!   server-side attestation we cannot fake, so Android classes collapse to
//!   `Chrome` (exactly what real WA Web on Chrome-Android emits).
//! - `companion_platform_display` is `<Browser> (<OS>)` / `Android (<OS>)` with
//!   the OS canonicalized to a server-safe set; see [`CompanionOs`].

use wacore::companion_reg::CompanionWebClientType;
use wacore::pair_code::PairCodeOptions;
use waproto::whatsapp::device_props::PlatformType;

/// Official `PlatformType` → pairing identity.
///
/// Returns the `CompanionWebClientType` to emit as `companion_platform_id` and
/// the canonical OS label for `companion_platform_display`.
pub fn pairing_identity(pt: PlatformType) -> (CompanionWebClientType, &'static str) {
    use CompanionWebClientType as C;
    use PlatformType as P;
    match pt {
        // Android classes collapse to Chrome: the Android letters ('d'/'e'/'f')
        // need server-side attestation we can't fake, and Chrome-Android is what
        // real WA Web emits anyway.
        P::ANDROID_PHONE | P::ANDROID_TABLET | P::ANDROID_AMBIGUOUS => (C::Chrome, "Android"),
        P::IOS_PHONE | P::IPAD | P::IOS_CATALYST => (C::OtherWebClient, "iOS"),
        P::DESKTOP => (C::Electron, "Windows"),
        P::CHROME => (C::Chrome, "Linux"),
        P::FIREFOX => (C::Firefox, "Linux"),
        P::EDGE => (C::Edge, "Windows"),
        P::SAFARI => (C::Safari, "Linux"),
        P::OPERA => (C::Opera, "Linux"),
        P::IE => (C::Ie, "Windows"),
        P::UWP => (C::Uwp, "Windows"),
        // TV, wearables, VR, AR, cloud API, smartglasses, WAIL ... fall back to
        // the generic web client — safest server-accepted identity.
        _ => (C::OtherWebClient, "Linux"),
    }
}

/// `companion_platform_display`, e.g. `"Chrome (Android)"` for ANDROID_PHONE.
/// The OS is canonicalized through [`CompanionOs`] because the pair-code server
/// rejects a non-OS display string with `bad-request`.
pub fn platform_display(pt: PlatformType) -> String {
    let (ct, os) = pairing_identity(pt);
    wacore::companion_reg::companion_platform_display(ct, os)
}

/// Apply this platform's pairing identity to a `PairCodeOptions`.
pub fn apply_to_pair_options(pt: PlatformType, opts: &mut PairCodeOptions) {
    let (ct, os) = pairing_identity(pt);
    opts.platform_id = Some(ct);
    opts.display_os = Some(os.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn android_collapses_to_chrome_not_android_letter() {
        // 'e' (ANDROID_PHONE) needs attestation we can't fake; must emit Chrome.
        let (ct, os) = pairing_identity(PlatformType::ANDROID_PHONE);
        assert_eq!(ct, CompanionWebClientType::Chrome);
        assert_eq!(os, "Android");
    }

    #[test]
    fn desktop_maps_to_electron() {
        let (ct, os) = pairing_identity(PlatformType::DESKTOP);
        assert_eq!(ct, CompanionWebClientType::Electron);
        assert_eq!(os, "Windows");
    }

    #[test]
    fn display_uses_canonical_os() {
        assert_eq!(
            platform_display(PlatformType::ANDROID_PHONE),
            "Chrome (Android)"
        );
        assert_eq!(platform_display(PlatformType::DESKTOP), "Chrome (Windows)");
        assert_eq!(platform_display(PlatformType::CHROME), "Chrome (Linux)");
        assert_eq!(platform_display(PlatformType::IOS_PHONE), "Chrome (iOS)");
    }

    #[test]
    fn unknown_falls_back_to_web() {
        let (ct, os) = pairing_identity(PlatformType::TCL_TV);
        assert_eq!(ct, CompanionWebClientType::OtherWebClient);
        assert_eq!(os, "Linux");
    }
}
