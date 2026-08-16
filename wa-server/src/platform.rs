//! Business-side platform mapping.
//!
//! `biz.wa_user.platform` stores the **official** WhatsApp `PlatformType`
//! (0-25) as the source of truth. The business-side `x_platform` (1=android,
//! 2=ios, 3=web, 4=windows, 5=macos) is a client convenience; this module maps
//! between the two and derives the pairing-time `companion_platform_id` +
//! `companion_platform_display` from the official type.
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

/// Business-side client type. 1=android, 2=ios, 3=web, 4=windows, 5=macos.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XPlatform {
    Android,
    Ios,
    Web,
    Windows,
    Macos,
}

impl XPlatform {
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            1 => Some(Self::Android),
            2 => Some(Self::Ios),
            3 => Some(Self::Web),
            4 => Some(Self::Windows),
            5 => Some(Self::Macos),
            _ => None,
        }
    }

    /// Canonical OS hint used for `companion_platform_display` / `ClientProfile`.
    pub fn os_hint(self) -> &'static str {
        match self {
            Self::Android => "Android",
            Self::Ios => "iOS",
            Self::Web => "Linux",
            Self::Windows => "Windows",
            Self::Macos => "Mac OS",
        }
    }

    /// Official WhatsApp `PlatformType` (stored in `biz.wa_user.platform`).
    pub fn platform_type(self) -> PlatformType {
        match self {
            Self::Android => PlatformType::ANDROID_PHONE,
            Self::Ios => PlatformType::IOS_PHONE,
            Self::Web => PlatformType::CHROME,
            Self::Windows | Self::Macos => PlatformType::DESKTOP,
        }
    }

    /// `CompanionWebClientType` for pairing. Android deliberately collapses to
    /// `Chrome` (Android letters need attestation we can't fake); iOS/others
    /// also fall back to browser/desktop IDs the server accepts.
    pub fn companion_type(self) -> CompanionWebClientType {
        match self {
            Self::Android => CompanionWebClientType::Chrome,
            Self::Ios => CompanionWebClientType::OtherWebClient,
            Self::Web => CompanionWebClientType::Chrome,
            Self::Windows | Self::Macos => CompanionWebClientType::Electron,
        }
    }

    /// Pairing-time `companion_platform_display`, e.g. `"Chrome (Android)"`.
    pub fn platform_display(self) -> String {
        companion_platform_display(self.companion_type(), self.os_hint())
    }

    /// Populate a `PairCodeOptions` with this platform's identity.
    pub fn apply_to_pair_options(self, opts: &mut PairCodeOptions) {
        opts.platform_id = Some(self.companion_type());
        opts.display_os = Some(self.os_hint().to_string());
    }
}

/// Derive an [`XPlatform`] from an official `PlatformType`. Unrecognized types
/// (TV, wearables, VR, ...) fall back to `Web` — safest generic client.
pub fn x_platform_from_platform_type(pt: PlatformType) -> XPlatform {
    match pt {
        PlatformType::ANDROID_PHONE
        | PlatformType::ANDROID_TABLET
        | PlatformType::ANDROID_AMBIGUOUS => XPlatform::Android,
        PlatformType::IOS_PHONE | PlatformType::IPAD | PlatformType::IOS_CATALYST => XPlatform::Ios,
        PlatformType::DESKTOP => XPlatform::Web, // desktop Windows/macOS both land here
        _ => XPlatform::Web,
    }
}

/// `companion_platform_display` with the OS canonicalized through
/// [`CompanionOs`] — the pair-code server rejects a non-OS display string.
pub fn companion_platform_display(ct: CompanionWebClientType, os: &str) -> String {
    use wacore::companion_reg::companion_platform_display as canonical;
    canonical(ct, os)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x_platform_roundtrip() {
        assert_eq!(XPlatform::from_i16(1), Some(XPlatform::Android));
        assert_eq!(XPlatform::from_i16(5), Some(XPlatform::Macos));
        assert_eq!(XPlatform::from_i16(0), None);
        assert_eq!(XPlatform::from_i16(6), None);
    }

    #[test]
    fn official_types_map_correctly() {
        assert_eq!(
            XPlatform::Android.platform_type(),
            PlatformType::ANDROID_PHONE
        );
        assert_eq!(XPlatform::Ios.platform_type(), PlatformType::IOS_PHONE);
        assert_eq!(XPlatform::Web.platform_type(), PlatformType::CHROME);
        assert_eq!(XPlatform::Windows.platform_type(), PlatformType::DESKTOP);
        assert_eq!(XPlatform::Macos.platform_type(), PlatformType::DESKTOP);
    }

    #[test]
    fn companion_type_is_server_safe() {
        // Android must NOT emit 'e' (ANDROID_PHONE) — attestation can't be faked.
        assert_eq!(
            XPlatform::Android.companion_type(),
            CompanionWebClientType::Chrome
        );
        assert_eq!(
            XPlatform::Windows.companion_type(),
            CompanionWebClientType::Electron
        );
        assert_eq!(
            XPlatform::Macos.companion_type(),
            CompanionWebClientType::Electron
        );
    }

    #[test]
    fn display_uses_canonical_os() {
        assert_eq!(XPlatform::Android.platform_display(), "Chrome (Android)");
        assert_eq!(XPlatform::Windows.platform_display(), "Chrome (Windows)");
        assert_eq!(XPlatform::Macos.platform_display(), "Chrome (Mac OS)");
        assert_eq!(XPlatform::Web.platform_display(), "Chrome (Linux)");
    }
}
