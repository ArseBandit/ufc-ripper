//! Opt-in replay followers. Search results are only candidates: the normal
//! manifest and download path still checks the subscriber's access.

use std::{
    collections::HashSet,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{interval, MissedTickBehavior};
use ufcr_libs::{log_err, log_info, log_warn};

use crate::{
    bin_util::start_download,
    config_util::{get_config, CONFIG_PATH},
    net_util::{get_vod_manifest, search_replay_vods},
    state_util::get_dlq,
    txt_util::create_uuid,
    ws_util::{emit_auto_download_started, emit_config_update, emit_vod_download_progress},
};

const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);
const MAX_PAGES: u64 = 10;

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct FollowState {
    handled_ids: HashSet<u64>,
    handled_titles: HashSet<String>,
    contender_initialized: bool,
    fight_night_initialized: bool,
    fight_night_baseline: String,
    fight_night_latest_event: String,
    ufc_initialized_number: u16,
    highest_ufc_number: u16,
    bjj_initialized_number: u16,
    highest_bjj_number: u16,
}

#[derive(Clone, Debug)]
struct Replay {
    id: u64,
    title: String,
    published: String,
}

pub fn start() {
    tokio::spawn(async {
        let mut timer = interval(POLL_INTERVAL);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let state_path = CONFIG_PATH.with_file_name("auto_follow_state.json");
        let stored = match fs::read(&state_path) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(state) => state,
                Err(error) => {
                    log_err!("Replay follower state is invalid; automation paused: {error}");
                    return;
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => FollowState::default(),
            Err(error) => {
                log_err!("Replay follower state cannot be read; automation paused: {error}");
                return;
            }
        };
        let state = Arc::new(Mutex::new(stored));
        loop {
            timer.tick().await;
            let (ufc_number, bjj_number, contender, fight_nights, include_existing) = {
                let config = get_config();
                if !config.auto_follow_replays || config.auth_token.is_empty() {
                    continue;
                }
                (
                    config.auto_follow_ufc_number,
                    config.auto_follow_bjj_number,
                    config.auto_follow_contender_series,
                    config.auto_follow_fight_nights,
                    config.auto_follow_existing,
                )
            };
            if let Err(error) = poll(
                &state,
                &state_path,
                ufc_number,
                bjj_number,
                contender,
                fight_nights,
                include_existing,
            )
            .await
            {
                log_warn!("Replay follower check failed: {error}");
            }
        }
    });
}

async fn poll(
    state: &Arc<Mutex<FollowState>>,
    path: &std::path::Path,
    ufc_number: u16,
    bjj_number: u16,
    contender: bool,
    fight_nights: bool,
    include_existing: bool,
) -> anyhow::Result<()> {
    // Never add an automated download while a manual or another automated
    // download is active. Subsequent ticks pick up the remaining recordings.
    if get_dlq().values().any(|vod| vod.status == "downloading") {
        return Ok(());
    }

    if ufc_number > 0 {
        {
            let mut guard = state.lock().unwrap();
            if guard.ufc_initialized_number != ufc_number {
                guard.highest_ufc_number = ufc_number.saturating_sub(1);
            }
        }
        let highest = {
            let current = state.lock().unwrap().highest_ufc_number;
            current.max(ufc_number)
        };
        let initial = state.lock().unwrap().ufc_initialized_number != ufc_number;
        let first = highest.saturating_sub(2).max(ufc_number);
        for number in first..=highest.saturating_add(1) {
            let replays = search_replays(&format!("\"UFC {number}\""), |name, duration| {
                classify_ufc(name, duration, number)
            })
            .await?;
            {
                let mut guard = state.lock().unwrap();
                if initial && !include_existing {
                    guard.handled_ids.extend(replays.iter().map(|item| item.id));
                    guard
                        .handled_titles
                        .extend(replays.iter().map(|item| item.title.clone()));
                }
                guard.ufc_initialized_number = ufc_number;
                if number > guard.highest_ufc_number && !replays.is_empty() {
                    guard.highest_ufc_number = number;
                }
                save_state(path, &guard)?;
            }
            if !(initial && !include_existing) {
                for replay in replays {
                    if queue_one(state, path, replay).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    if bjj_number > 0 {
        {
            let mut guard = state.lock().unwrap();
            if guard.bjj_initialized_number != bjj_number {
                guard.highest_bjj_number = bjj_number.saturating_sub(1);
            }
        }
        let highest = {
            let current = state.lock().unwrap().highest_bjj_number;
            current.max(bjj_number)
        };
        let initial = state.lock().unwrap().bjj_initialized_number != bjj_number;
        let first = highest.saturating_sub(2).max(bjj_number);
        for number in first..=highest.saturating_add(1) {
            let replays = search_replays(&format!("\"UFC BJJ {number}\""), |name, duration| {
                classify_bjj(name, duration, number)
            })
            .await?;
            {
                let mut guard = state.lock().unwrap();
                if initial && !include_existing {
                    guard.handled_ids.extend(replays.iter().map(|item| item.id));
                    guard
                        .handled_titles
                        .extend(replays.iter().map(|item| item.title.clone()));
                }
                guard.bjj_initialized_number = bjj_number;
                if number > guard.highest_bjj_number && !replays.is_empty() {
                    guard.highest_bjj_number = number;
                }
                save_state(path, &guard)?;
            }
            if !(initial && !include_existing) {
                for replay in replays {
                    if queue_one(state, path, replay).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    if contender {
        let mut replays =
            search_replays("\"Dana White's Contender Series\"", classify_contender).await?;
        replays.sort_by(|a, b| b.published.cmp(&a.published));
        {
            let mut guard = state.lock().unwrap();
            if !guard.contender_initialized {
                let keep = if include_existing { 1 } else { 0 };
                guard
                    .handled_ids
                    .extend(replays.iter().skip(keep).map(|item| item.id));
                guard
                    .handled_titles
                    .extend(replays.iter().skip(keep).map(|item| item.title.clone()));
                guard.contender_initialized = true;
                save_state(path, &guard)?;
            }
        }
        for replay in replays {
            if queue_one(state, path, replay).await? {
                return Ok(());
            }
        }
    }
    if fight_nights {
        let mut replays =
            search_replays_limited("\"UFC Fight Night:\"", classify_fight_night, true).await?;
        if replays.is_empty() {
            return Ok(());
        }
        replays.sort_by(|a, b| b.published.cmp(&a.published).then_with(|| b.id.cmp(&a.id)));
        {
            let mut guard = state.lock().unwrap();
            if !guard.fight_night_initialized {
                guard.fight_night_baseline = replays[0].published.clone();
                let latest = if include_existing {
                    replays
                        .first()
                        .and_then(|item| fight_night_key(&item.title))
                } else {
                    None
                };
                guard.fight_night_latest_event = latest.unwrap_or("").to_string();
                for replay in &replays {
                    if fight_night_key(&replay.title) == latest && latest.is_some() {
                        continue;
                    }
                    guard.handled_ids.insert(replay.id);
                    guard.handled_titles.insert(replay.title.clone());
                }
                guard.fight_night_initialized = true;
                save_state(path, &guard)?;
            }
        }
        for replay in replays {
            let guard = state.lock().unwrap();
            let older = !guard.fight_night_baseline.is_empty()
                && replay.published < guard.fight_night_baseline
                && fight_night_key(&replay.title) != Some(guard.fight_night_latest_event.as_str());
            drop(guard);
            if older {
                continue;
            }
            if queue_one(state, path, replay).await? {
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn search_replays(
    query: &str,
    classify: impl Fn(&str, u64) -> bool,
) -> anyhow::Result<Vec<Replay>> {
    search_replays_limited(query, classify, false).await
}

async fn search_replays_limited(
    query: &str,
    classify: impl Fn(&str, u64) -> bool,
    allow_truncated: bool,
) -> anyhow::Result<Vec<Replay>> {
    let mut found = Vec::new();
    for page in 0..MAX_PAGES {
        let result = search_replay_vods(query, page).await?;
        let pages = result["nbPages"].as_u64().unwrap_or(0);
        if pages > MAX_PAGES || result["nbHits"].as_u64().unwrap_or(0) > MAX_PAGES * 100 {
            if !allow_truncated {
                anyhow::bail!("Replay search exceeded the safe page limit");
            }
            if page == 0 {
                log_warn!("Fight Night search has more than {MAX_PAGES} pages; only the first {MAX_PAGES} are checked");
            }
        }
        let hits = result["hits"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid replay results"))?;
        for hit in hits {
            if let Some(replay) = candidate(hit, &classify) {
                found.push(replay);
            }
        }
        if page + 1 >= pages {
            break;
        }
    }
    Ok(found)
}

fn candidate(hit: &Value, classify: &impl Fn(&str, u64) -> bool) -> Option<Replay> {
    let name = hit["name"].as_str()?;
    let duration = hit["duration"].as_u64()?;
    if !classify(name, duration) {
        return None;
    }
    Some(Replay {
        id: hit["id"].as_u64()?,
        title: name.to_string(),
        published: hit["publishedDate"].as_str().unwrap_or("").to_string(),
    })
}

fn classify_ufc(name: &str, duration: u64, number: u16) -> bool {
    let Some(suffix) = event_suffix(name, &format!("UFC {number}: ")) else {
        return false;
    };
    if suffix == "Prelims" || suffix == "Fight Pass Prelims" {
        return duration >= 3600;
    }
    duration >= 5400
        && suffix.contains(" vs ")
        && !suffix.contains('(')
        && !suffix.contains(" - ")
        && !suffix.contains("Promo")
}

fn classify_contender(name: &str, duration: u64) -> bool {
    if duration < 3600 {
        return false;
    }
    let Some(rest) = name.strip_prefix("Dana White's Contender Series: Season ") else {
        return false;
    };
    let Some((season, week)) = rest.split_once(", Week ") else {
        return false;
    };
    !season.is_empty()
        && season.bytes().all(|ch| ch.is_ascii_digit())
        && !week.is_empty()
        && week.bytes().all(|ch| ch.is_ascii_digit())
}

fn fight_night_key(name: &str) -> Option<&str> {
    let suffix = event_suffix(name, "UFC Fight Night: ")?;
    let base = [" Fight Pass Prelims", " Early Prelims", " Prelims"]
        .iter()
        .find_map(|ending| suffix.strip_suffix(ending))
        .unwrap_or(suffix);
    if base.contains(" vs ")
        && !base.contains('(')
        && !base.contains(" - ")
        && ![
            "Promo",
            "Press Conference",
            "Highlights",
            "Recap",
            "Countdown",
            "Interview",
            "Weigh-In",
            "Preview",
            "Full Fight",
        ]
        .iter()
        .any(|word| base.contains(word))
    {
        Some(base)
    } else {
        None
    }
}

fn classify_fight_night(name: &str, duration: u64) -> bool {
    if fight_night_key(name).is_none() {
        return false;
    }
    let Some(suffix) = event_suffix(name, "UFC Fight Night: ") else {
        return false;
    };
    if [" Fight Pass Prelims", " Early Prelims", " Prelims"]
        .iter()
        .any(|ending| suffix.ends_with(ending))
    {
        duration >= 3600
    } else {
        duration >= 5400
    }
}

fn classify_bjj(name: &str, duration: u64, number: u16) -> bool {
    let Some(suffix) = event_suffix(name, &format!("UFC BJJ {number}: ")) else {
        return false;
    };
    duration >= 3600 && suffix.contains(" vs ") && !suffix.contains('(') && !suffix.contains(" - ")
}

fn event_suffix<'a>(name: &'a str, marker: &str) -> Option<&'a str> {
    let index = name.find(marker)?;
    let prefix = &name[..index];
    if !prefix.is_empty() {
        let sponsor_like = prefix.ends_with(' ')
            && prefix.len() <= 32
            && !prefix.contains(" - ")
            && !prefix.to_ascii_lowercase().contains(" vs ")
            && prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | ' ' | '&' | '\''));
        let language_word = [
            "portuguese",
            "english",
            "espanol",
            "francais",
            "deutsch",
            "italiano",
        ]
        .iter()
        .any(|word| prefix.to_ascii_lowercase().contains(word));
        if !sponsor_like || language_word {
            return None;
        }
    }
    Some(&name[index + marker.len()..])
}

async fn queue_one(
    state: &Arc<Mutex<FollowState>>,
    path: &std::path::Path,
    replay: Replay,
) -> anyhow::Result<bool> {
    if {
        let guard = state.lock().unwrap();
        guard.handled_ids.contains(&replay.id) || guard.handled_titles.contains(&replay.title)
    } {
        return Ok(false);
    }
    let mut vod = get_vod_manifest(replay.id, true).await?;
    if !vod.access {
        return Ok(false);
    }
    if existing_media(&get_config().dl_path, &vod.title) {
        let mut guard = state.lock().unwrap();
        guard.handled_ids.insert(replay.id);
        guard.handled_titles.insert(replay.title);
        save_state(path, &guard)?;
        return Ok(false);
    }
    vod.q_id = create_uuid();
    let id = replay.id;
    let title = replay.title.clone();
    let state_done = Arc::clone(state);
    let path_done = path.to_path_buf();
    let queued = start_download(
        &vod,
        false,
        |q_id, updates| {
            emit_vod_download_progress(q_id, updates);
        },
        move |q_id| {
            let mut guard = state_done.lock().unwrap();
            guard.handled_ids.insert(id);
            guard.handled_titles.insert(title);
            if let Err(error) = save_state(&path_done, &guard) {
                log_err!("Could not save completed replay ID: {error}");
            }
            emit_vod_download_progress(q_id, serde_json::json!({"status":"completed"}));
        },
        |q_id, error| {
            log_warn!("Automatic replay download failed: {error}");
            emit_vod_download_progress(q_id, serde_json::json!({"status":"failed"}));
        },
    )
    .await?;
    emit_auto_download_started(&queued);
    emit_config_update();
    log_info!("Queued replay: {}", replay.title);
    Ok(true)
}

fn existing_media(dir: &str, title: &str) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        let stem = path.file_stem().and_then(|part| part.to_str());
        let numbered_title = stem.and_then(|stem| stem.split_once(". "));
        path.is_file()
            && (stem == Some(title)
                || numbered_title.is_some_and(|(prefix, remainder)| {
                    prefix.bytes().all(|ch| ch.is_ascii_digit()) && remainder == title
                }))
            && matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("mp4" | "mkv" | "mov" | "avi" | "webm")
            )
    })
}

fn save_state(path: &std::path::Path, state: &FollowState) -> anyhow::Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec(state)?)?;
    if let Err(error) = fs::rename(&tmp, path) {
        if !path.exists() {
            return Err(error.into());
        }
        // Windows cannot rename a file over an existing destination.
        fs::remove_file(path)?;
        fs::rename(&tmp, path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        candidate, classify_bjj, classify_contender, classify_fight_night, classify_ufc,
        existing_media, fight_night_key, save_state, FollowState,
    };
    use serde_json::json;

    #[test]
    fn selects_english_event_segments_and_rejects_extras() {
        assert!(classify_ufc("UFC 321: Aspinall vs Gane", 10811, 321));
        assert!(classify_ufc("UFC 321: Prelims", 13733, 321));
        assert!(classify_ufc("UFC 321: Fight Pass Prelims", 7769, 321));
        assert!(classify_ufc(
            "Crypto.com UFC 331: Van vs Pantoja 2",
            11335,
            331
        ));
        assert!(classify_ufc("Crypto.com UFC 331: Prelims", 7346, 331));
        assert!(classify_ufc(
            "Crypto.com UFC 331: Fight Pass Prelims",
            5389,
            331
        ));
        assert!(!classify_ufc(
            "Crypto.com UFC 331: Post-Fight Press Conference",
            7346,
            331
        ));
        assert!(!classify_ufc(
            "Crypto.com UFC 331: Van vs Pantoja 2 (Español)",
            11335,
            331
        ));
        assert!(!classify_ufc(
            "(ESPAÑOL) UFC 331: Van vs Pantoja 2",
            11335,
            331
        ));
        assert!(!classify_ufc(
            "Portuguese UFC 331: Van vs Pantoja 2",
            11335,
            331
        ));
        assert!(!classify_ufc(
            "Tom Aspinall vs Ciryl Gane UFC 321",
            944,
            321
        ));
        assert!(!classify_ufc(
            "UFC 321: Aspinall vs Gane - Fight Promo",
            80,
            321
        ));
        assert!(!classify_ufc(
            "UFC 321: Аспинэлл vs Ган (русские комментаторы)",
            25286,
            321
        ));
        assert!(!classify_ufc(
            "UFC 321: Post-Fight Press Conference",
            4197,
            321
        ));
        assert!(!classify_ufc("UFC 322: Prelims", 14000, 321));
    }

    #[test]
    fn selects_complete_contender_episodes_only() {
        assert!(classify_contender(
            "Dana White's Contender Series: Season 10, Week 7",
            10223
        ));
        assert!(!classify_contender(
            "Dana White's Contender Series: Season 10, Week 7 Highlights",
            9000
        ));
        assert!(!classify_contender(
            "Magomed Zaynukov vs Lucas Caldas DWCS",
            1242
        ));
        assert!(!classify_contender(
            "Dana White's Contender Series: Season 10, Week 8",
            300
        ));
    }

    #[test]
    fn selects_full_bjj_events_only() {
        assert!(classify_bjj("UFC BJJ 9: Fowler vs Johnson", 7200, 9));
        assert!(classify_bjj(
            "Crypto.com UFC BJJ 11: Smith vs Jones",
            7200,
            11
        ));
        assert!(!classify_bjj(
            "(ESPAÑOL) UFC BJJ 11: Smith vs Jones",
            7200,
            11
        ));
        assert!(!classify_bjj("UFC BJJ 9: Fowler vs Johnson", 600, 9));
        assert!(!classify_bjj("Fowler vs Johnson UFC BJJ 9", 7200, 9));
        assert!(!classify_bjj("UFC BJJ 10: Smith vs Jones", 7200, 9));
    }

    #[test]
    fn selects_full_fight_night_segments_only() {
        assert!(classify_fight_night(
            "UFC Fight Night: Bautista vs Oliveira",
            9800
        ));
        assert!(classify_fight_night(
            "UFC Fight Night: Du Plessis vs Usman Prelims",
            7200
        ));
        assert!(classify_fight_night(
            "UFC Fight Night: Du Plessis vs Usman Early Prelims",
            5400
        ));
        assert!(classify_fight_night(
            "Crypto.com UFC Fight Night: Royval vs Kape Fight Pass Prelims",
            5400
        ));
        assert_eq!(
            fight_night_key("UFC Fight Night: Du Plessis vs Usman Prelims"),
            Some("Du Plessis vs Usman")
        );
        assert_eq!(
            fight_night_key("UFC Fight Night: Du Plessis vs Usman"),
            Some("Du Plessis vs Usman")
        );
        assert!(!classify_fight_night(
            "UFC Fight Night: Bautista vs Oliveira Highlights",
            9000
        ));
        assert!(!classify_fight_night(
            "UFC Fight Night: Bautista vs Oliveira Post-Fight Press Conference",
            7200
        ));
        assert!(!classify_fight_night(
            "(ESPAÑOL) UFC Fight Night: Bautista vs Oliveira",
            9000
        ));
        assert!(!classify_fight_night(
            "UFC Fight Night: Bautista vs Oliveira (RUS)",
            9000
        ));
        assert!(!classify_fight_night(
            "Alex Morono vs Daniil Donchenko UFC Fight Night: Bautista vs Oliveira",
            9000
        ));
        assert!(!classify_fight_night(
            "UFC Fight Night: Bautista vs Oliveira",
            1200
        ));
    }

    #[test]
    fn catalogue_hit_requires_id_duration_and_exact_title() {
        let hit = json!({"id":1024887,"name":"Dana White's Contender Series: Season 10, Week 7", "duration":10223});
        assert_eq!(candidate(&hit, &classify_contender).unwrap().id, 1024887);
        assert!(candidate(
            &json!({"name":"Dana White's Contender Series: Season 10, Week 7", "duration":10223}),
            &classify_contender
        )
        .is_none());
    }

    #[test]
    fn completed_ids_and_titles_survive_restart() {
        let path =
            std::env::temp_dir().join(format!("ufcr-follow-test-{}.json", std::process::id()));
        let mut state = FollowState::default();
        state.handled_ids.insert(1024887);
        state
            .handled_titles
            .insert("Dana White's Contender Series: Season 10, Week 7".into());
        save_state(&path, &state).unwrap();
        let restored: FollowState = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(restored.handled_ids.contains(&1024887));
        assert!(restored
            .handled_titles
            .contains("Dana White's Contender Series: Season 10, Week 7"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn old_follower_state_leaves_fight_nights_uninitialized() {
        let state: FollowState = serde_json::from_value(json!({
            "handled_ids": [1024887],
            "handled_titles": ["Dana White's Contender Series: Season 10, Week 7"],
            "contender_initialized": true
        }))
        .unwrap();
        assert!(!state.fight_night_initialized);
        assert!(state.fight_night_baseline.is_empty());
        assert!(state.handled_ids.contains(&1024887));
    }

    #[test]
    fn completed_media_is_recognized_but_parts_are_not() {
        let dir = std::env::temp_dir().join(format!("ufcr-media-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let title = "UFC 321 - Prelims";
        std::fs::write(dir.join(format!("{title}.mp4.part")), b"").unwrap();
        assert!(!existing_media(dir.to_str().unwrap(), title));
        std::fs::write(dir.join(format!("{title}.mp4")), b"complete").unwrap();
        assert!(existing_media(dir.to_str().unwrap(), title));
        std::fs::remove_file(dir.join(format!("{title}.mp4"))).unwrap();
        std::fs::write(dir.join(format!("42. {title}.mkv")), b"complete").unwrap();
        assert!(existing_media(dir.to_str().unwrap(), title));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
