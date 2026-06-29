const express    = require('express');
const cors       = require('cors');
const { io }     = require('socket.io-client');
const fetch      = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors());
app.use(express.json());

// ── GEMINI AI via REST — no SDK, no dependency issues ──
async function askAI(systemPrompt, userPrompt) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) { console.log('[AI] GEMINI_API_KEY not set'); return null; }
  try {
    const prompt = `${systemPrompt}\n\nUser message: ${userPrompt}\n\nYour reply (plain text only, no markdown, max 2 sentences):`;
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`,
      {
        method:  'POST',
        headers: { 'content-type': 'application/json' },
        body:    JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { maxOutputTokens: 200, temperature: 0.8, thinkingConfig: { thinkingBudget: 0 } }
        })
      }
    );
    const data = await res.json();
    if (!res.ok) {
      console.error('[AI] Gemini API error:', res.status, JSON.stringify(data).slice(0, 200));
      return null;
    }
    const parts = data?.candidates?.[0]?.content?.parts || [];
    const text = parts.filter(p => p.text && !p.thought).map(p => p.text).join(' ').trim();
    const finishReason = data?.candidates?.[0]?.finishReason;
    if (finishReason && finishReason !== 'STOP') {
      console.log(`[AI] finishReason=${finishReason}, text so far: ${text?.slice(0, 80)}`);
    }
    console.log(`[AI] OK: ${text?.slice(0, 80)}`);
    return text || null;
  } catch (e) {
    console.error('[AI] Fetch error:', e.message);
    return null;
  }
}

// ── CONFIG ──
const CLIENT_ID     = process.env.BLAZE_CLIENT_ID     || 'UR4ghwgTTJ2rAE1_KBZgmCKmkvQtO4ux';
const CLIENT_SECRET = process.env.BLAZE_CLIENT_SECRET || '';
const API           = 'https://api.blaze.stream/v1';
const startedAt     = Date.now();

// ── BOT ACCOUNT (single identity across all channels) ──
const BOT_SESSION_TOKEN  = process.env.BLAZE_SESSION_TOKEN     || '';
let   BOT_API_TOKEN      = process.env.BLAZE_BOT_TOKEN         || '';
let   BOT_REFRESH_TOKEN  = process.env.BLAZE_BOT_REFRESH_TOKEN || '';
let   BOT_TOKEN          = BOT_API_TOKEN;
const BOT_CHANNEL_ID     = process.env.BLAZE_BOT_CHANNEL_ID    || '6b0971e0-548c-447a-bb71-e2fa62369d18';
let   BOT_USERNAME       = process.env.BLAZE_BOT_USERNAME       || 'blazeguy';
let   tokenRefreshing    = false;

// ── AUTO REFRESH TOKEN ──
async function refreshBotToken() {
  if (tokenRefreshing) return false;
  if (!BOT_REFRESH_TOKEN) { console.error('[BOT] No refresh token set — cannot refresh'); return false; }
  tokenRefreshing = true;
  console.log('[BOT] Refreshing access token...');
  try {
    const res  = await fetch('https://blaze.stream/bapi/oauth2/refresh', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        clientId:     CLIENT_ID,
        clientSecret: CLIENT_SECRET,
        refreshToken: BOT_REFRESH_TOKEN
      })
    });
    const data = await res.json();
    if (data.accessToken) {
      BOT_API_TOKEN = data.accessToken;
      BOT_TOKEN     = data.accessToken;
      if (data.refreshToken) BOT_REFRESH_TOKEN = data.refreshToken;
      // Update all connected channel sockets to use new token
      Object.values(sockets).forEach(s => { if (s.token === BOT_TOKEN || !s.token) s.token = BOT_API_TOKEN; });
      console.log(`[BOT] ✅ Token refreshed! New length: ${BOT_API_TOKEN.length}`);
      tokenRefreshing = false;
      return true;
    } else {
      console.error('[BOT] Token refresh failed:', JSON.stringify(data));
      tokenRefreshing = false;
      return false;
    }
  } catch (e) {
    console.error('[BOT] Token refresh error:', e.message);
    tokenRefreshing = false;
    return false;
  }
}

// Wrapper around fetch that auto-refreshes on 401
async function blazeFetch(url, options, retry = true) {
  const res = await fetch(url, options);
  if (res.status === 401 && retry) {
    console.log(`[BOT] 401 on ${url} — attempting token refresh`);
    const refreshed = await refreshBotToken();
    if (refreshed) {
      // Retry with new token
      options.headers['authorization'] = `Bearer ${BOT_API_TOKEN}`;
      return fetch(url, options);
    }
  }
  return res;
}

// Auto-refresh every 20 minutes proactively
setInterval(async () => {
  if (BOT_REFRESH_TOKEN) {
    console.log('[BOT] Proactive token refresh...');
    await refreshBotToken();
  }
}, 20 * 60 * 1000);

// Supabase — set these in Render env vars
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ── SCORING ──
const SCORES     = { msg: 1, follow: 10, sub: 50, gift: 30, vote: 5 };
const MILESTONES = [
  { pts: 10,   name: 'Lurker',  icon: '👀' },
  { pts: 100,  name: 'Regular', icon: '💬' },
  { pts: 250,  name: 'Devoted', icon: '⭐' },
  { pts: 500,  name: 'Fanatic', icon: '🔥' },
  { pts: 1000, name: 'Diamond', icon: '💎' },
  { pts: 2000, name: 'Legend',  icon: '👑' },
];

// ── IN-MEMORY CHANNEL SOCKETS ──
// sockets[channelId] = { socket, sessionId, connected, reconnectTimer }
const sockets = {};

// ── TIMED MESSAGES ENGINE ──
// timedMsgTimers[channelId] = [intervalIds...]
const timedMsgTimers = {};

async function startTimedMessages(channelId, token) {
  // Clear existing
  if (timedMsgTimers[channelId]) timedMsgTimers[channelId].forEach(t => clearInterval(t));
  timedMsgTimers[channelId] = [];

  const { data: msgs } = await supabase
    .from('timed_messages')
    .select('*')
    .eq('channel_id', channelId)
    .eq('active', true);

  if (!msgs?.length) return;
  for (const m of msgs) {
    const ms = m.interval_mins * 60 * 1000;
    const t  = setInterval(async () => {
      await sendChat(channelId, token, m.message);
      await supabase.from('timed_messages').update({ last_sent_at: new Date().toISOString() }).eq('id', m.id);
    }, ms);
    timedMsgTimers[channelId].push(t);
    console.log(`[${channelId.slice(0,8)}] Timed msg every ${m.interval_mins}min: "${m.message.slice(0,40)}"`);
  }
}

// ── KNOWN SPAM BOT USERNAMES ──
const SPAM_BOT_PATTERNS = [
  /follow.*for.*follow/i, /sub.*for.*sub/i, /check.*my.*channel/i,
  /buy.*followers/i, /free.*subs/i, /\bviews\b.*\bsale\b/i,
];

function isSpam(message) {
  return SPAM_BOT_PATTERNS.some(p => p.test(message));
}

// ── CHANNEL SETTINGS CACHE ──
const settingsCache = {};
async function getSettings(channelId) {
  if (settingsCache[channelId]) return settingsCache[channelId];
  const { data } = await supabase.from('channel_settings').select('*').eq('channel_id', channelId).single();
  const defaults = { currency_name: 'points', tracked_phrase: null, spam_protection: true, brb_active: false, events_on: true };
  settingsCache[channelId] = data || defaults;
  return settingsCache[channelId];
}
async function updateSettings(channelId, patch) {
  settingsCache[channelId] = { ...(settingsCache[channelId] || {}), ...patch };
  await supabase.from('channel_settings').upsert({ channel_id: channelId, ...patch, updated_at: new Date().toISOString() }, { onConflict: 'channel_id' });
}

// ── HELPERS ──
function getMilestone(score) {
  let m = MILESTONES[0];
  for (const ms of MILESTONES) { if (score >= ms.pts) m = ms; }
  return m;
}
function getNextMilestone(score) {
  return MILESTONES.find(ms => score < ms.pts) || null;
}
function blazeHeaders(token) {
  return {
    'client-id': CLIENT_ID,
    'authorization': `Bearer ${token}`,
    'content-type': 'application/json',
    'accept': 'application/json'
  };
}

// ── SUPABASE HELPERS ──
async function getChannelToken(channelId) {
  const { data } = await supabase
    .from('channels')
    .select('token')
    .eq('id', channelId)
    .single();
  return data?.token || null;
}

async function upsertLoyalty(channelId, uid, username, displayName, avatarUrl, type, amount, meta = {}) {
  // Fetch current row
  const { data: existing } = await supabase
    .from('loyalty')
    .select('*')
    .eq('channel_id', channelId)
    .eq('user_id', uid)
    .single();

  const pts    = SCORES[type] * amount;
  const key    = type === 'msg' ? 'msgs' : type + 's';
  const prev   = existing ? getMilestone(existing.score) : MILESTONES[0];
  const score  = (existing?.score || 0) + pts;
  const next   = getMilestone(score);

  const row = {
    channel_id:    channelId,
    user_id:       uid,
    username:      username      || existing?.username,
    display_name:  displayName   || existing?.display_name,
    avatar_url:    avatarUrl     || existing?.avatar_url,
    score,
    [key]:         (existing?.[key] || 0) + amount,
    is_subscriber: meta.isSubscriber ?? existing?.is_subscriber ?? false,
    is_follower:   meta.isFollower   ?? existing?.is_follower   ?? false,
    milestone:     next.name,
    last_seen:     new Date().toISOString(),
    ...(existing ? {} : { first_seen: new Date().toISOString() }),
  };

  await supabase.from('loyalty').upsert(row, { onConflict: 'channel_id,user_id' });

  // Milestone crossed?
  if (next.pts > prev.pts) {
    const name = displayName || username;
    await pushAlert(channelId, 'milestone',
      `${name} reached ${next.icon} ${next.name} (${score} pts)!`, '🏆');
    const token = await getChannelToken(channelId);
    if (token) await sendChat(channelId, token,
      `🏆 ${next.icon} @${username} just reached ${next.name} status with ${score} loyalty points! Congrats!`);
  }
}

async function pushAlert(channelId, type, message, icon) {
  await supabase.from('alerts').insert({ channel_id: channelId, type, message, icon });
}

// ── BLAZE CHAT ──
async function sendChat(channelId, token, message) {
  try {
    const headers = blazeHeaders(BOT_API_TOKEN || token);
    const res = await blazeFetch(`${API}/chats/messages`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ channelId, message })
    });
    if (!res.ok) {
      const txt = await res.text();
      console.error(`[${channelId.slice(0,8)}] sendChat failed ${res.status}: ${txt.slice(0,80)}`);
    } else {
      console.log(`[${channelId.slice(0,8)}] SENT: ${message.slice(0,60)}`);
    }
  } catch (e) {
    console.error(`[${channelId.slice(0,8)}] sendChat error:`, e.message);
  }
}

// ── BOT FOLLOW USER ──
// ── BOT FOLLOW USER ──
// Uses BLAZE_SESSION_TOKEN (browser cookie token) — this is separate from OAuth.
// Session tokens DO rotate when the bot account logs out or sessions expire.
// If follow stops working, get a fresh token from browser cookies while logged in as blazeguy.
async function botFollowChannel(targetChannelId) {
  // Session token rotates — always use latest from env, fallback to OAuth token
  const sessionToken = process.env.BLAZE_SESSION_TOKEN || BOT_API_TOKEN;
  try {
    const res = await fetch(`https://blaze.stream/bapi/channels/${targetChannelId}/follow`, {
      method: 'POST',
      headers: {
        'authorization':  `Bearer ${sessionToken}`,
        'content-type':   'application/json',
        'content-length': '0',
        'origin':  'https://blaze.stream',
        'referer': 'https://blaze.stream/',
      },
      body: ''
    });
    const text = await res.text();
    console.log(`[BOT] Follow ${targetChannelId.slice(0,8)} → ${res.status}: ${text.slice(0,120)}`);
    if (res.status === 401) {
      console.warn('[BOT] ⚠ Session token expired — update BLAZE_SESSION_TOKEN in Render env vars');
    }
    return res.ok;
  } catch (e) {
    console.error(`[BOT] Follow error:`, e.message);
    return false;
  }
}

// ── JOIN: streamer types !join in blazeguy's channel ──
async function handleJoin(sender) {
  const { id: streamerId, username, displayName, avatarUrl } = sender;
  console.log(`[BOT] !join from @${username} (${streamerId})`);

  if (!BOT_TOKEN) {
    console.error('[BOT] BLAZE_BOT_TOKEN not set!');
    return;
  }

  // Don't let the bot join itself
  if (streamerId === BOT_CHANNEL_ID) return;

  // Check not already joined
  const { data: existing } = await supabase
    .from('channels').select('id').eq('id', streamerId).single();
  if (existing) {
    await sendChat(BOT_CHANNEL_ID, BOT_TOKEN,
      `@${username} ⚡ @${BOT_USERNAME} is already in your channel! Type !leave in your chat to remove me.`);
    return;
  }

  // Save to Supabase
  const { error: upsertErr } = await supabase.from('channels').upsert({
    id: streamerId, token: BOT_TOKEN,
    username, display_name: displayName, avatar_url: avatarUrl,
    last_seen: new Date().toISOString()
  }, { onConflict: 'id' });
  if (upsertErr) console.error('[BOT] Channel upsert error:', upsertErr.message);

  // Init channel settings
  await supabase.from('channel_settings').upsert({
    channel_id: streamerId, currency_name: 'points',
    spam_protection: true, events_on: true
  }, { onConflict: 'channel_id' });

  // Follow the streamer so bot can post in follower-only chat
  const followed = await botFollowChannel(streamerId);
  console.log(`[BOT] Follow result for @${username}: ${followed}`);

  // Connect socket to their channel
  connectChannel(streamerId, BOT_TOKEN);

  // Confirm in blazeguy's channel
  await sendChat(BOT_CHANNEL_ID, BOT_TOKEN,
    `✅ @${username} ⚡ @${BOT_USERNAME} has joined your channel and followed you! Now type /mod ${BOT_USERNAME} in your chat to unlock full features!`);

  // Welcome in streamer's channel — wait for socket confirmed connected
  let attempts = 0;
  const welcomeInterval = setInterval(async () => {
    attempts++;
    const sock = sockets[streamerId];
    if (sock?.connected || attempts >= 15) {
      clearInterval(welcomeInterval);
      if (sock?.connected) {
        await sendChat(streamerId, BOT_TOKEN,
          `⚡ @${BOT_USERNAME} has joined the chat! Loyalty tracking is ON 🏆 I'm now tracking your most devoted viewers. Type !commands to see everything I can do!`);
      } else {
        console.error(`[BOT] Socket for @${username} never connected after 15s`);
      }
    }
  }, 1000);

  console.log(`[BOT] ✅ Joined @${username} (${streamerId})`);
}

// ── LEAVE: works from streamer's OWN chat OR from blazeguy's chat ──
async function handleLeave(channelId, sender) {
  const username = sender.username || sender;

  // Figure out the actual streamer channel to leave
  // If typed in blazeguy's channel → leave the sender's channel
  // If typed in their own channel → leave that channel
  const targetChannelId = (channelId === BOT_CHANNEL_ID) ? sender.id : channelId;
  const targetUsername  = username;

  // Don't let bot leave its own channel
  if (targetChannelId === BOT_CHANNEL_ID) return;

  // Send goodbye in streamer's channel FIRST (before disconnecting)
  try {
    await sendChat(targetChannelId, BOT_TOKEN,
      `👋 @${BOT_USERNAME} is leaving this channel. Your loyalty data is saved forever — type !join in @${BOT_USERNAME}'s chat anytime to bring me back! ⚡`);
  } catch {}

  // Small delay so goodbye message sends before socket closes
  await new Promise(r => setTimeout(r, 1500));

  // Disconnect socket
  const sock = sockets[targetChannelId];
  if (sock) {
    clearTimeout(sock.reconnectTimer);
    if (sock.socket) sock.socket.disconnect();
    delete sockets[targetChannelId];
  }

  // Clear timed message timers
  if (timedMsgTimers[targetChannelId]) {
    timedMsgTimers[targetChannelId].forEach(t => clearInterval(t));
    delete timedMsgTimers[targetChannelId];
  }

  // Remove from Supabase (bot stops rejoining on restart)
  // NOTE: we do NOT unfollow — blazeguy stays following the streamer
  await supabase.from('channels').delete().eq('id', targetChannelId);

  // Confirm in blazeguy's channel
  await sendChat(BOT_CHANNEL_ID, BOT_TOKEN,
    `👋 @${BOT_USERNAME} has left @${targetUsername}'s channel. Data saved. Type !join here to re-add anytime!`);

  console.log(`[BOT] Left channel @${targetUsername} (${targetChannelId})`);
}

// ── SUBSCRIPTIONS ──
async function createSubscriptions(channelId, token, sessionId) {
  const types = [
    'channel.chat.message', 'channel.follow', 'channel.unfollow',
    'channel.subscribe', 'channel.subscription.gift', 'channel.vote',
    'channel.ban', 'channel.unban', 'channel.raid',
    'channel.moderate', 'stream.online', 'stream.offline',
  ];
  for (const type of types) {
    try {
      const headers = blazeHeaders(BOT_API_TOKEN || token);
      const res  = await blazeFetch(`${API}/events/subscriptions`, {
        method: 'POST', headers,
        body: JSON.stringify({ type, version: '1', sessionId, condition: { channelId } })
      });
      const data = await res.json();
      console.log(`[${channelId.slice(0,8)}] SUB ${type}:`, data.id ? 'ok' : JSON.stringify(data).slice(0,80));
    } catch (e) {
      console.error(`[${channelId.slice(0,8)}] SUB error:`, e.message);
    }
  }
}

// ── MINI GAME STATE (in-memory per channel, resets on restart — intentional, keeps it fresh) ──
// cooldowns[channelId][userId][game] = timestamp of last play
const cooldowns = {};
// activeTrivia[channelId] = { answer, reward, expiresAt, askedBy }
const activeTrivia = {};
// activeHeist[channelId] = { pot, members[], expiresAt }
const activeHeist  = {};

const COOLDOWN_MS = 30 * 1000; // 30 seconds between games per user

function canPlay(channelId, userId, game) {
  const now = Date.now();
  const last = cooldowns[channelId]?.[userId]?.[game] || 0;
  return now - last >= COOLDOWN_MS;
}
function setCooldown(channelId, userId, game) {
  if (!cooldowns[channelId]) cooldowns[channelId] = {};
  if (!cooldowns[channelId][userId]) cooldowns[channelId][userId] = {};
  cooldowns[channelId][userId][game] = Date.now();
}

const TRIVIA_QUESTIONS = [
  { q: 'How many sides does a hexagon have?',         a: '6',          hint: 'hex = 6' },
  { q: 'What planet is closest to the Sun?',          a: 'mercury',    hint: 'starts with M' },
  { q: 'What color do you get mixing red and blue?',  a: 'purple',     hint: 'it\'s royal' },
  { q: 'How many minutes in an hour?',                a: '60',         hint: 'think clock' },
  { q: 'What is 7 x 8?',                              a: '56',         hint: 'close to 55' },
  { q: 'What animal is the king of the jungle?',      a: 'lion',       hint: 'big cat' },
  { q: 'How many days in a week?',                    a: '7',          hint: 'one for each day' },
  { q: 'What is the capital of France?',              a: 'paris',      hint: 'Eiffel Tower city' },
  { q: 'What is H2O?',                                a: 'water',      hint: 'you drink it' },
  { q: 'How many seconds in a minute?',               a: '60',         hint: 'same as minutes in an hour' },
  { q: 'What is the fastest land animal?',            a: 'cheetah',    hint: 'big spotted cat' },
  { q: 'What gas do plants absorb?',                  a: 'co2',        hint: 'carbon something' },
  { q: 'How many legs does a spider have?',           a: '8',          hint: 'more than 6' },
  { q: 'What is 100 divided by 4?',                   a: '25',         hint: 'quarter of 100' },
  { q: 'What color is the sky on a clear day?',       a: 'blue',       hint: 'cool color' },
];

// ── OWNER-ONLY COMMANDS ──
const OWNER_ONLY_CMDS = new Set([
  '!addcmd','!delcmd','!additem','!setcurrency','!setphrase',
  '!addtimer','!stoptimer','!starttimer','!deltimer',
  '!spam','!events','!endpoll','!back','!aimod'
]);

// ── BOT COMMANDS ──
async function handleCommand(channelId, token, message, sender) {
  const parts = message.trim().split(/\s+/);
  const cmd   = parts[0].toLowerCase();
  const arg   = parts.slice(1).join(' ').replace('@', '').trim();

  const say = (msg) => sendChat(channelId, BOT_API_TOKEN || token, msg);

  // Owner check — sender.id matches channelId, or sender is the dashboard
  const isOwner = sender.id === channelId || sender.isOwner === true || sender.username === 'dashboard';

  if (OWNER_ONLY_CMDS.has(cmd) && !isOwner) {
    await say(`🔒 @${sender.username} — only the streamer can use ${cmd}`);
    return;
  }

  switch (cmd) {
    case '!devoted': {
      const { data: top } = await supabase
        .from('loyalty').select('username,score')
        .eq('channel_id', channelId)
        .order('score', { ascending: false }).limit(5);
      if (!top?.length) { await say(`💬 No loyalty data yet — start chatting to earn points!`); return; }
      const medals = ['🥇','🥈','🥉','4️⃣','5️⃣'];
      const lines  = top.map((u, i) => `${medals[i]} @${u.username} · ${u.score}pts`);
      await say(`🏆 Most Devoted Viewers ─ ${lines.join(' ┃ ')}`);
      break;
    }
    case '!loyalty': {
      const target = arg || sender.username;
      const { data: u } = await supabase
        .from('loyalty').select('*')
        .eq('channel_id', channelId)
        .eq('username', target).single();
      if (!u) { await say(`❓ @${target} hasn't earned any points yet — start chatting!`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      const { count } = await supabase.from('loyalty')
        .select('*', { count: 'exact', head: true })
        .eq('channel_id', channelId).gt('score', u.score);
      const rank = (count || 0) + 1;
      const nextStr = next ? `📈 ${next.pts - u.score} pts to ${next.icon} ${next.name}` : `👑 MAX TIER`;
      await say(`${ms.icon} @${u.username} ─ Rank #${rank} ┃ ${u.score} pts ┃ ${ms.name} ┃ ${nextStr}`);
      break;
    }
    case '!shoutout': {
      if (!arg) { await say(`📣 Usage: !shoutout @username`); return; }
      const { data: u } = await supabase
        .from('loyalty').select('*')
        .eq('channel_id', channelId)
        .eq('username', arg).single();
      if (!u) {
        await say(`📣 Shoutout to @${arg}! Go check them out on Blaze ⚡ blaze.stream/${arg}`);
        return;
      }
      const ms = getMilestone(u.score);
      await say(`📣 Shoutout to @${u.username}! ${ms.icon} ${ms.name} ┃ ${u.score} pts ┃ ${u.msgs || 0} messages ┃ Check them out → blaze.stream/${u.username} ⚡`);
      break;
    }
    case '!milestone': {
      const target = arg || sender.username;
      const { data: u } = await supabase
        .from('loyalty').select('score,username')
        .eq('channel_id', channelId)
        .eq('username', target).single();
      if (!u) { await say(`❓ @${target} not found — they need to chat first!`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      if (!next) { await say(`👑 @${u.username} is a LEGEND — highest tier reached! Maximum devotion! ⚡`); return; }
      const pct  = Math.round((u.score / next.pts) * 100);
      await say(`${ms.icon} @${u.username} ─ ${u.score}/${next.pts} pts ┃ ${pct}% to ${next.icon} ${next.name} ┃ Need ${next.pts - u.score} more pts!`);
      break;
    }
    case '!leaderboard': {
      const { data: top } = await supabase
        .from('loyalty').select('username,score')
        .eq('channel_id', channelId)
        .order('score', { ascending: false }).limit(3);
      if (!top?.length) { await say(`💬 No data yet — start chatting to earn points!`); return; }
      const lines = top.map((u, i) => `${['🥇','🥈','🥉'][i]} @${u.username} · ${u.score}pts (${getMilestone(u.score).name})`);
      await say(`⚡ blazeguy Loyalty Board ─ ${lines.join(' ┃ ')} ─ Type !loyalty to check your rank!`);
      break;
    }
    case '!hug': {
      if (!arg) { await say(`🤗 Usage: !hug @username`); return; }
      const { data: u } = await supabase
        .from('loyalty').select('score').eq('channel_id', channelId).eq('username', arg).single();
      const ms = getMilestone(u?.score || 0);
      await say(`🤗 Sending a big hug to @${arg}! ${ms.icon} ${ms.name} member of this amazing community! 💛`);
      break;
    }
    case '!uptime': {
      const mins  = Math.round((Date.now() - startedAt) / 60000);
      const hours = Math.floor(mins / 60);
      const remMins = mins % 60;
      const timeStr = hours > 0 ? `${hours}h ${remMins}m` : `${mins}m`;
      const { count } = await supabase
        .from('loyalty').select('*', { count: 'exact', head: true })
        .eq('channel_id', channelId);
      await say(`⚡ blazeguy ─ Online for ${timeStr} ┃ Tracking ${count || 0} viewers ┃ Type !commands to see all features!`);
      break;
    }

    // ── MINI GAMES ──

    case '!coinflip': {
      // Usage: !coinflip heads 20  — bet 1-50 pts on heads/tails
      const side = parts[1]?.toLowerCase();
      const bet  = Math.min(50, Math.max(1, parseInt(parts[2]) || 10));
      if (!side || !['heads','tails','h','t'].includes(side)) {
        await say(`🪙 Usage: !coinflip heads 20 (or tails) — bet 1-50 pts. 30s cooldown.`); return;
      }
      if (!canPlay(channelId, sender.id, 'coinflip')) {
        await say(`⏳ @${sender.username} cooldown active! Wait 30s between games.`); return;
      }
      // Check they have enough pts
      const { data: pdata } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!pdata || pdata.score < bet) { await say(`@${sender.username} not enough pts! You have ${pdata?.score||0} pts.`); return; }

      const flip   = Math.random() < 0.5 ? 'heads' : 'tails';
      const picked = ['h','heads'].includes(side) ? 'heads' : 'tails';
      const won    = flip === picked;
      const delta  = won ? bet : -bet;
      setCooldown(channelId, sender.id, 'coinflip');

      await supabase.from('loyalty').update({ score: pdata.score + delta }).eq('channel_id', channelId).eq('user_id', sender.id);
      await say(won
        ? `🪙 @${sender.username} flipped ${flip} — YOU WIN! +${bet} pts 🎉 (${pdata.score + delta} total)`
        : `🪙 @${sender.username} flipped ${flip} — you lose! -${bet} pts 😬 (${pdata.score + delta} total)`);
      break;
    }

    case '!roll': {
      // !roll 20 — roll a dice 1-N, top half wins double pts, bottom half loses
      const sides = Math.min(100, Math.max(2, parseInt(parts[1]) || 6));
      const bet   = Math.min(50, Math.max(1, parseInt(parts[2]) || 10));
      if (!canPlay(channelId, sender.id, 'roll')) {
        await say(`⏳ @${sender.username} wait 30s between rolls!`); return;
      }
      const { data: rdata } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!rdata || rdata.score < bet) { await say(`@${sender.username} not enough pts!`); return; }

      const roll = Math.floor(Math.random() * sides) + 1;
      const won  = roll > sides / 2;
      const delta = won ? bet : -bet;
      setCooldown(channelId, sender.id, 'roll');

      await supabase.from('loyalty').update({ score: rdata.score + delta }).eq('channel_id', channelId).eq('user_id', sender.id);
      await say(won
        ? `🎲 @${sender.username} rolled ${roll}/${sides} — HIGH ROLL! +${bet} pts 🎉 (${rdata.score + delta} total)`
        : `🎲 @${sender.username} rolled ${roll}/${sides} — low roll. -${bet} pts 😬 (${rdata.score + delta} total)`);
      break;
    }

    case '!rps': {
      // !rps rock 15
      const choices  = ['rock','paper','scissors'];
      const picked   = parts[1]?.toLowerCase();
      const bet      = Math.min(50, Math.max(1, parseInt(parts[2]) || 10));
      if (!picked || !choices.includes(picked)) {
        await say(`✊ Usage: !rps rock/paper/scissors 20 — bet 1-50 pts`); return;
      }
      if (!canPlay(channelId, sender.id, 'rps')) {
        await say(`⏳ @${sender.username} wait 30s!`); return;
      }
      const { data: rpsdata } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!rpsdata || rpsdata.score < bet) { await say(`@${sender.username} not enough pts!`); return; }

      const bot   = choices[Math.floor(Math.random() * 3)];
      const wins  = { rock:'scissors', paper:'rock', scissors:'paper' };
      const won   = wins[picked] === bot;
      const draw  = picked === bot;
      const delta = won ? bet : draw ? 0 : -bet;
      const icons = { rock:'✊', paper:'🖐', scissors:'✌️' };
      setCooldown(channelId, sender.id, 'rps');

      await supabase.from('loyalty').update({ score: rpsdata.score + delta }).eq('channel_id', channelId).eq('user_id', sender.id);
      const result = won ? `YOU WIN! +${bet} pts 🎉` : draw ? `DRAW! No change 🤝` : `you lose! -${bet} pts 😬`;
      await say(`${icons[picked]} vs ${icons[bot]} — @${sender.username} ${result} (${rpsdata.score + delta} total)`);
      break;
    }

    case '!trivia': {
      // Streamer or any user starts trivia — first correct answer wins pts
      if (activeTrivia[channelId] && activeTrivia[channelId].expiresAt > Date.now()) {
        await say(`❓ Trivia already active! Answer the current question.`); return;
      }
      const q       = TRIVIA_QUESTIONS[Math.floor(Math.random() * TRIVIA_QUESTIONS.length)];
      const reward  = 25;
      activeTrivia[channelId] = { answer: q.a, hint: q.hint, reward, expiresAt: Date.now() + 30000, askedBy: sender.username };
      await say(`❓ TRIVIA (+${reward} pts to first correct answer): ${q.q} — You have 30 seconds! Hint if stuck: type !hint`);
      // Auto-expire
      setTimeout(async () => {
        if (activeTrivia[channelId]?.answer === q.a) {
          delete activeTrivia[channelId];
          await sendChat(channelId, token, `⏰ Time's up! The answer was: ${q.a}`);
        }
      }, 30000);
      break;
    }

    case '!hint': {
      const t = activeTrivia[channelId];
      if (!t || t.expiresAt < Date.now()) { await say(`No active trivia right now! Type !trivia to start one.`); return; }
      await say(`💡 Hint: ${t.hint}`);
      break;
    }

    case '!heist': {
      // !heist start — anyone can join, bot picks a random win/loss after 30s
      if (activeHeist[channelId] && activeHeist[channelId].expiresAt > Date.now()) {
        // Join existing heist
        const h = activeHeist[channelId];
        if (h.members.find(m => m.id === sender.id)) { await say(`@${sender.username} you're already in the heist!`); return; }
        const bet = Math.min(50, Math.max(1, parseInt(parts[1]) || 15));
        const { data: hd } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
        if (!hd || hd.score < bet) { await say(`@${sender.username} not enough pts to join!`); return; }
        h.members.push({ id: sender.id, username: sender.username, bet });
        h.pot += bet;
        await say(`🦹 @${sender.username} joined the heist with ${bet} pts! Total pot: ${h.pot} pts. Type !heist to join!`);
        return;
      }
      // Start new heist
      const bet = Math.min(50, Math.max(1, parseInt(parts[1]) || 15));
      const { data: hd } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!hd || hd.score < bet) { await say(`@${sender.username} not enough pts!`); return; }
      activeHeist[channelId] = { pot: bet, members: [{ id: sender.id, username: sender.username, bet }], expiresAt: Date.now() + 30000 };
      await say(`🦹 @${sender.username} started a HEIST! Type !heist [bet] to join! 30 seconds to recruit the crew. Pot: ${bet} pts`);

      setTimeout(async () => {
        const h = activeHeist[channelId];
        if (!h) return;
        delete activeHeist[channelId];
        if (h.members.length < 2) {
          await sendChat(channelId, token, `🦹 Not enough crew for the heist! @${h.members[0].username} gets their bet back.`);
          return;
        }
        const success = Math.random() < 0.55; // 55% chance of success
        if (success) {
          const winPer = Math.floor(h.pot / h.members.length);
          for (const m of h.members) {
            const { data: md } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', m.id).single();
            if (md) await supabase.from('loyalty').update({ score: md.score + winPer }).eq('channel_id', channelId).eq('user_id', m.id);
          }
          const names = h.members.map(m => `@${m.username}`).join(', ');
          await sendChat(channelId, token, `🎊 HEIST SUCCESS! ${names} each win ${winPer} pts from the ${h.pot} pt pot! 💰`);
        } else {
          for (const m of h.members) {
            const { data: md } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', m.id).single();
            if (md) await supabase.from('loyalty').update({ score: Math.max(0, md.score - m.bet) }).eq('channel_id', channelId).eq('user_id', m.id);
          }
          const names = h.members.map(m => `@${m.username}`).join(', ');
          await sendChat(channelId, token, `🚨 HEIST FAILED! The crew got caught! ${names} each lost their bet. 😬`);
        }
      }, 30000);
      break;
    }

    case '!games': {
      await say(`🎮 Games: !coinflip heads/tails [bet] ┃ !roll [sides] [bet] ┃ !rps rock/paper/scissors [bet] ┃ !trivia (25pts) ┃ !heist [bet] ┃ !dice [sides] ┃ !8ball question — max 50pt bet, 30s cooldown`);
      break;
    }

    case '!rank': {
      const target = arg || sender.username;
      const { data: u } = await supabase.from('loyalty').select('*').eq('channel_id', channelId).eq('username', target).single();
      if (!u) { await say(`❓ @${target} hasn't earned any points yet!`); return; }
      const ms   = getMilestone(u.score);
      const next = getNextMilestone(u.score);
      const { count } = await supabase.from('loyalty').select('*', { count:'exact', head:true }).eq('channel_id', channelId).gt('score', u.score);
      const rank = (count || 0) + 1;
      await say(`${ms.icon} @${u.username} ─ Rank #${rank} ┃ ${u.score} pts ┃ ${ms.name}${next ? ` ┃ ${next.pts - u.score} pts to ${next.icon} ${next.name}` : ' ┃ 👑 MAX'}`);
      break;
    }
    case '!top': {
      const { data: top } = await supabase.from('loyalty').select('username,score').eq('channel_id', channelId).order('score', { ascending: false }).limit(3);
      if (!top?.length) { await say(`💬 No data yet — chat to earn points!`); return; }
      await say(`⚡ Top viewers ─ ${top.map((u,i)=>`${['🥇','🥈','🥉'][i]} @${u.username} (${u.score}pts)`).join(' ┃ ')}`);
      break;
    }
    case '!give': {
      const targetUser = parts[1]?.replace('@','');
      const amount     = parseInt(parts[2]);
      if (!targetUser || !amount || amount < 1) { await say(`💸 Usage: !give @username amount`); return; }
      if (targetUser.toLowerCase() === sender.username.toLowerCase()) { await say(`😅 @${sender.username} you can't give points to yourself!`); return; }
      const { data: from } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!from || from.score < amount) { await say(`@${sender.username} not enough points! You have ${from?.score || 0} pts.`); return; }
      const { data: to } = await supabase.from('loyalty').select('user_id,score').eq('channel_id', channelId).eq('username', targetUser).single();
      if (!to) { await say(`❓ @${targetUser} not found — they need to chat first!`); return; }
      await supabase.from('loyalty').update({ score: from.score - amount }).eq('channel_id', channelId).eq('user_id', sender.id);
      await supabase.from('loyalty').update({ score: to.score + amount }).eq('channel_id', channelId).eq('user_id', to.user_id);
      await say(`💸 @${sender.username} gave ${amount} pts to @${targetUser}! ┃ @${sender.username}: ${from.score - amount}pts ┃ @${targetUser}: ${to.score + amount}pts`);
      break;
    }
    case '!8ball': {
      if (!arg) { await say(`🎱 Usage: !8ball will I win today?`); return; }
      const answers = ['✅ Yes, definitely!','✅ Without a doubt!','✅ Signs point to yes!','✅ Most likely!','🤔 Ask again later...','🤔 Cannot predict now...','❌ Don\'t count on it.','❌ My sources say no.','❌ Very doubtful.'];
      await say(`🎱 @${sender.username} asks: "${arg}" ─ ${answers[Math.floor(Math.random() * answers.length)]}`);
      break;
    }
    case '!dice': {
      const sides = Math.min(100, Math.max(2, parseInt(parts[1]) || 6));
      await say(`🎲 @${sender.username} rolled a d${sides} and got: ${Math.floor(Math.random() * sides) + 1}!`);
      break;
    }
    case '!quote': {
      if (arg) {
        await supabase.from('alerts').insert({ channel_id: channelId, type: 'quote', message: `"${arg}" — @${sender.username}`, icon: '💬' });
        await say(`💬 Quote saved: "${arg}"`);
      } else {
        const { data: quotes } = await supabase.from('alerts').select('message').eq('channel_id', channelId).eq('type', 'quote').limit(50);
        if (!quotes?.length) { await say(`💬 No quotes saved yet! Use: !quote your text here`); return; }
        const pick = quotes[Math.floor(Math.random() * quotes.length)];
        await say(`💬 ${pick.message}`);
      }
      break;
    }
    case '!stats': {
      const target = arg || sender.username;
      const { data: u } = await supabase.from('loyalty').select('*').eq('channel_id', channelId).eq('username', target).single();
      if (!u) { await say(`❓ @${target} has no stats yet!`); return; }
      await say(`📊 @${u.username} ─ ${u.score}pts ┃ ${u.msgs||0} msgs ┃ ${u.follows||0} follows ┃ ${u.subs||0} subs ┃ ${u.gifts||0} gifts ┃ ${u.votes||0} votes ┃ ${getMilestone(u.score).icon} ${getMilestone(u.score).name}`);
      break;
    }
    case '!addcmd': {
      const trigger  = parts[1]?.toLowerCase().startsWith('!') ? parts[1].toLowerCase() : '!' + (parts[1]||'').toLowerCase();
      const response = parts.slice(2).join(' ');
      if (!parts[1] || !response) { await say('Usage: !addcmd !trigger response text'); return; }
      await supabase.from('custom_commands').upsert({ channel_id: channelId, trigger, response, created_by: sender.username }, { onConflict: 'channel_id,trigger' });
      await say(`✅ Command ${trigger} added!`);
      break;
    }
    case '!delcmd': {
      const trigger = parts[1]?.toLowerCase().startsWith('!') ? parts[1].toLowerCase() : '!' + (parts[1]||'').toLowerCase();
      await supabase.from('custom_commands').delete().eq('channel_id', channelId).eq('trigger', trigger);
      await say(`🗑 Command ${trigger} deleted.`);
      break;
    }
    case '!commands': {
      const { data: cmds } = await supabase.from('custom_commands').select('trigger').eq('channel_id', channelId).limit(10);
      const custom = cmds?.map(c => c.trigger).join(' ') || 'none yet';
      await say(`⚡ blazeguy Commands ─ 🏆 Loyalty: !devoted !loyalty !rank !top !leaderboard !milestone !balance !stats !shoutout !give ─ 🎮 Games: !coinflip !roll !rps !8ball !dice !trivia !heist ─ 🛒 Shop: !shop !buy ─ 🤖 AI: !ask !roast !compliment !predict !story !recap ─ 🔧 Tools: !brb !poll !so !hug !quote !uptime ─ 📋 Custom: ${custom}`);
      break;
    }

    // ── LOYALTY SHOP ──
    case '!shop': {
      const { data: items } = await supabase.from('shop_items').select('*').eq('channel_id', channelId).eq('active', true);
      if (!items?.length) { await say('🛒 Shop empty! Add items with !additem slug name cost'); return; }
      const settings = await getSettings(channelId);
      const list = items.map(i => `${i.name} (${i.cost} ${settings.currency_name}, !buy ${i.slug}${i.stock > 0 ? `, ${i.stock} left` : ''})`).join(' | ');
      await say(`🛒 SHOP: ${list}`);
      break;
    }
    case '!buy': {
      if (!arg) { await say('Usage: !buy <slug>'); return; }
      const { data: item } = await supabase.from('shop_items').select('*').eq('channel_id', channelId).eq('slug', arg).eq('active', true).single();
      if (!item) { await say(`@${sender.username} item "${arg}" not found. Type !shop`); return; }
      const { data: buyer } = await supabase.from('loyalty').select('score').eq('channel_id', channelId).eq('user_id', sender.id).single();
      if (!buyer || buyer.score < item.cost) { await say(`@${sender.username} not enough points! Need ${item.cost}, you have ${buyer?.score||0}.`); return; }
      if (item.stock === 0) { await say(`@${sender.username} "${item.name}" is sold out!`); return; }
      await supabase.from('loyalty').update({ score: buyer.score - item.cost }).eq('channel_id', channelId).eq('user_id', sender.id);
      await supabase.from('shop_purchases').insert({ channel_id: channelId, user_id: sender.id, username: sender.username, slug: item.slug, cost: item.cost });
      if (item.stock > 0) await supabase.from('shop_items').update({ stock: item.stock - 1 }).eq('id', item.id);
      const settings = await getSettings(channelId);
      await say(`🛍 @${sender.username} redeemed "${item.name}" for ${item.cost} ${settings.currency_name}! Balance: ${buyer.score - item.cost} pts 🎉`);
      break;
    }
    case '!additem': {
      if (parts.length < 4) { await say('Usage: !additem slug Name cost — e.g. !additem shoutout "Channel Shoutout" 200'); return; }
      const slug = parts[1].toLowerCase();
      const cost = parseInt(parts[parts.length - 1]);
      const name = parts.slice(2, parts.length - 1).join(' ');
      if (isNaN(cost)) { await say('Cost must be a number.'); return; }
      await supabase.from('shop_items').upsert({ channel_id: channelId, slug, name, cost }, { onConflict: 'channel_id,slug' });
      await say(`✅ Shop item "${name}" added for ${cost} pts (!buy ${slug})`);
      break;
    }
    case '!balance': {
      const target = arg || sender.username;
      const { data: u } = await supabase.from('loyalty').select('score,username').eq('channel_id', channelId).eq('username', target).single();
      const settings = await getSettings(channelId);
      if (!u) { await say(`@${target} has no ${settings.currency_name} yet!`); return; }
      await say(`💰 @${u.username} — ${u.score} ${settings.currency_name} ${getMilestone(u.score).icon}`);
      break;
    }
    case '!setcurrency': {
      if (!arg) { await say('Usage: !setcurrency BlazeBucks'); return; }
      await updateSettings(channelId, { currency_name: arg });
      await say(`💰 Currency name set to "${arg}"!`);
      break;
    }

    // ── SHOUTOUT WITH CLIP ──
    case '!so': {
      if (!arg) { await say('Usage: !so @username'); return; }
      const target = arg.replace('@', '');
      try {
        const profileRes = await fetch(`${API}/users/profile?username=${target}`, { headers: blazeHeaders(token) });
        const profile    = await profileRes.json();
        const u          = profile.data || profile;
        const clipsRes   = await fetch(`${API}/channels/clips?channelId=${u.id||''}&limit=1&orderBy=most_recent`, { headers: blazeHeaders(token) });
        const clips      = await clipsRes.json();
        const clip       = clips.data?.[0];
        const clipText   = clip ? ` Latest clip: ${clip.url || clip.playbackUrl || ''}` : '';
        await say(`📣 Go show @${u.username || target} some love! 🔥 blaze.stream/${u.username || target}${clipText}`);
      } catch { await say(`📣 Go show @${target} some love! 🔥 blaze.stream/${target}`); }
      break;
    }

    // ── BRB ──
    case '!brb': {
      const mins = parseInt(parts[1]) || 5;
      await updateSettings(channelId, { brb_active: true });
      await say(`⏸ ${sender.username} is BRB for ~${mins} min! Hang tight chat ☕`);
      setTimeout(async () => {
        const s = await getSettings(channelId);
        if (s.brb_active) { await updateSettings(channelId, { brb_active: false }); await sendChat(channelId, token, `✅ We're back! Thanks for waiting chat ⚡`); }
      }, mins * 60 * 1000);
      break;
    }
    case '!back': {
      await updateSettings(channelId, { brb_active: false });
      await say(`✅ We're back! Welcome back chat ⚡`);
      break;
    }

    // ── POLLS ──
    case '!poll': {
      const raw   = parts.slice(1).join(' ');
      const split = raw.split('|').map(s => s.trim()).filter(Boolean);
      if (split.length < 3) { await say('Usage: !poll Question? | Option A | Option B | Option C'); return; }
      const question = split[0];
      const options  = split.slice(1);
      settingsCache[channelId] = settingsCache[channelId] || {};
      settingsCache[channelId]._poll = { question, options, votes: options.map(() => 0), voters: {}, active: true };
      const opts = options.map((o, i) => `${i + 1}. ${o}`).join(' | ');
      await say(`📊 POLL: ${question} — ${opts} | Vote by typing 1-${options.length}!`);
      setTimeout(async () => {
        const poll = settingsCache[channelId]?._poll;
        if (poll?.active) {
          poll.active = false;
          const results = poll.options.map((o, i) => `${o}: ${poll.votes[i]}`).join(' | ');
          const winner  = poll.options[poll.votes.indexOf(Math.max(...poll.votes))];
          await sendChat(channelId, token, `📊 POLL ENDED: "${winner}" wins! Results: ${results}`);
        }
      }, 2 * 60 * 1000);
      break;
    }
    case '!endpoll': {
      const poll = settingsCache[channelId]?._poll;
      if (!poll?.active) { await say('No active poll.'); return; }
      poll.active = false;
      const results = poll.options.map((o, i) => `${o}: ${poll.votes[i]}`).join(' | ');
      const winner  = poll.options[poll.votes.indexOf(Math.max(...poll.votes))];
      await say(`📊 POLL ENDED: "${winner}" wins! Results: ${results}`);
      break;
    }

    // ── PHRASE TRACKING ──
    case '!setphrase': {
      if (!arg) { await say('Usage: !setphrase yourphrase'); return; }
      await updateSettings(channelId, { tracked_phrase: arg.toLowerCase() });
      await say(`🎯 Now tracking phrase: "${arg}"!`);
      break;
    }
    case '!phrasestats': {
      const settings = await getSettings(channelId);
      if (!settings.tracked_phrase) { await say('No phrase set. Use !setphrase word'); return; }
      const { count } = await supabase.from('phrase_tracking').select('*', { count: 'exact', head: true }).eq('channel_id', channelId).eq('phrase', settings.tracked_phrase);
      const { data: top } = await supabase.from('phrase_tracking').select('username,count').eq('channel_id', channelId).eq('phrase', settings.tracked_phrase).order('count', { ascending: false }).limit(3);
      const topStr = top?.map((u, i) => `${['🥇','🥈','🥉'][i]} @${u.username} (${u.count}x)`).join(' ') || 'none yet';
      await say(`🎯 Phrase: "${settings.tracked_phrase}" | Total: ${count} uses | Top: ${topStr}`);
      break;
    }
    case '!phrasefirst': {
      const settings = await getSettings(channelId);
      if (!settings.tracked_phrase) { await say('No phrase set.'); return; }
      const { data: first } = await supabase.from('phrase_tracking').select('username,first_at').eq('channel_id', channelId).eq('phrase', settings.tracked_phrase).order('first_at', { ascending: true }).limit(1).single();
      if (!first) { await say('No one has said the phrase yet!'); return; }
      await say(`🎯 First to say "${settings.tracked_phrase}" was @${first.username} at ${new Date(first.first_at).toLocaleTimeString()}!`);
      break;
    }

    // ── TIMED MESSAGES ──
    case '!addtimer': {
      const mins = parseInt(parts[1]);
      const msg  = parts.slice(2).join(' ');
      if (!mins || !msg) { await say('Usage: !addtimer 30 Your timed message'); return; }
      await supabase.from('timed_messages').insert({ channel_id: channelId, message: msg, interval_mins: mins });
      await startTimedMessages(channelId, token);
      await say(`⏱ Timer added: "${msg.slice(0,40)}" every ${mins} min.`);
      break;
    }
    case '!timers': {
      const { data: timers } = await supabase.from('timed_messages').select('id,message,interval_mins,active').eq('channel_id', channelId);
      if (!timers?.length) { await say('No timers set. Use !addtimer mins message'); return; }
      await say(`⏱ Timers: ${timers.map(t => `[${t.id}] ${t.active?'✅':'⏸'} every ${t.interval_mins}min: "${t.message.slice(0,25)}"`).join(' | ')} — use !stoptimer [id] or !deltimer [id]`);
      break;
    }
    case '!stoptimer': {
      const id = parseInt(parts[1]);
      if (!id) { await say('Usage: !stoptimer [id] — get IDs with !timers'); return; }
      await supabase.from('timed_messages').update({ active: false }).eq('channel_id', channelId).eq('id', id);
      await startTimedMessages(channelId, token); // restart without this one
      await say(`⏸ Timer [${id}] paused. Use !starttimer ${id} to resume.`);
      break;
    }
    case '!starttimer': {
      const id = parseInt(parts[1]);
      if (!id) { await say('Usage: !starttimer [id]'); return; }
      await supabase.from('timed_messages').update({ active: true }).eq('channel_id', channelId).eq('id', id);
      await startTimedMessages(channelId, token);
      await say(`▶ Timer [${id}] resumed.`);
      break;
    }
    case '!deltimer': {
      const id = parseInt(parts[1]);
      if (!id) { await say('Usage: !deltimer [id] — get IDs with !timers'); return; }
      await supabase.from('timed_messages').delete().eq('channel_id', channelId).eq('id', id);
      await startTimedMessages(channelId, token);
      await say(`🗑 Timer [${id}] deleted permanently.`);
      break;
    }

    // ── SETTINGS ──
    case '!spam': {
      const on = parts[1]?.toLowerCase() === 'on';
      await updateSettings(channelId, { spam_protection: on });
      await say(`🛡 Spam protection ${on ? 'ENABLED' : 'DISABLED'}.`);
      break;
    }
    case '!events': {
      const on = parts[1]?.toLowerCase() === 'on';
      await updateSettings(channelId, { events_on: on });
      await say(`📣 Event announcements ${on ? 'ENABLED' : 'DISABLED'}.`);
      break;
    }

    // ── AI COMMANDS (powered by Groq / Llama 3) ──

    case '!ask': {
      if (!arg) { await say(`🤖 Usage: !ask your question — e.g. !ask what is a good stream schedule?`); return; }
      if (!process.env.GEMINI_API_KEY) { await say(`🤖 AI is not configured yet — GEMINI_API_KEY not set`); return; }
      const settings = await getSettings(channelId);
      await say(`🤖 Thinking…`);
      const answer = await askAI(
        `You are blazeguy, a fun helpful Blaze.stream chat bot. Keep answers SHORT (1-2 sentences max), casual, friendly. No markdown, no asterisks, plain text only. Channel loyalty currency: "${settings.currency_name}".`,
        arg, 100
      );
      if (answer) {
        await say(`🤖 @${sender.username} — ${answer}`);
      } else {
        await say(`🤖 @${sender.username} Gemini AI error — check GEMINI_API_KEY in Render env vars and try again!`);
      }
      break;
    }

    case '!roast': {
      if (!arg) { await say(`Usage: !roast @username`); return; }
      const target = arg.replace('@','');
      const { data: u } = await supabase.from('loyalty').select('score,msgs,username').eq('channel_id', channelId).eq('username', target).single();
      const context = u ? `They have ${u.score} loyalty points and sent ${u.msgs} messages.` : `They haven't chatted much.`;
      const roast = await askAI(
        `You are a funny, savage but friendly Blaze.stream chat bot called blazeguy. Write a single short roast (1 sentence, max 20 words) about a viewer. Keep it fun, not mean. No markdown.`,
        `Roast the viewer named "${target}". ${context}`,
        60
      );
      await say(roast ? `🔥 @${target} — ${roast}` : `🔥 @${target} is so mysterious even AI can't roast them!`);
      break;
    }

    case '!compliment': {
      const target = (arg || sender.username).replace('@','');
      const { data: u } = await supabase.from('loyalty').select('score,milestone').eq('channel_id', channelId).eq('username', target).single();
      const context = u ? `They have ${u.score} loyalty points and are a "${u.milestone}" member.` : '';
      const comp = await askAI(
        `You are blazeguy, a warm and hype Blaze.stream chat bot. Write a single genuine compliment (1 sentence, max 20 words) for a viewer. Energetic and fun. No markdown.`,
        `Compliment the viewer named "${target}". ${context}`,
        60
      );
      await say(comp ? `💛 @${target} — ${comp}` : `💛 @${target} is an absolute legend in this chat!`);
      break;
    }

    case '!aimod': {
      // Mod-only: checks if a message is toxic
      if (!arg) { await say(`Usage: !aimod [message to check]`); return; }
      const verdict = await askAI(
        `You are a chat moderation AI for a live stream. Analyse the message and reply with ONLY one of: SAFE, WARN, or BAN — then a single short reason (max 8 words). No markdown.`,
        `Message: "${arg}"`,
        40
      );
      await say(`🛡 AI Mod verdict: ${verdict || 'Unable to analyse'}`);
      break;
    }

    case '!recap': {
      // AI summary of current stream session
      const { data: topUsers } = await supabase.from('loyalty').select('username,score').eq('channel_id', channelId).order('score', { ascending: false }).limit(3);
      const { data: recentAlerts } = await supabase.from('alerts').select('type,message').eq('channel_id', channelId).order('created_at', { ascending: false }).limit(10);
      const topStr    = topUsers?.map((u,i) => `${i+1}. @${u.username} (${u.score} pts)`).join(', ') || 'no data';
      const alertsStr = recentAlerts?.map(a => a.message).join('; ') || 'no recent events';
      const recap = await askAI(
        `You are blazeguy, an energetic Blaze.stream chat bot. Write a fun 2-sentence stream recap for chat. Mention top viewers and recent events. Hype and positive. No markdown.`,
        `Top viewers: ${topStr}. Recent events: ${alertsStr}`,
        120
      );
      await say(recap ? `📋 Stream Recap: ${recap}` : `📋 Great stream! Check !devoted for your top fans!`);
      break;
    }

    case '!predict': {
      if (!arg) { await say(`Usage: !predict will we hit 100 followers today?`); return; }
      const { data: stats } = await supabase.from('loyalty').select('score.sum()', { count: 'exact' }).eq('channel_id', channelId);
      const prediction = await askAI(
        `You are blazeguy, a fun Blaze.stream chat bot. Give a short, fun, dramatic prediction (1-2 sentences, max 25 words). Be playful and entertaining. No markdown.`,
        `Prediction question: "${arg}"`,
        80
      );
      await say(prediction ? `🔮 ${prediction}` : `🔮 The crystal ball says... absolutely YES! Let's go! ⚡`);
      break;
    }

    case '!story': {
      // Generate a short story featuring chat members
      const { data: top } = await supabase.from('loyalty').select('username').eq('channel_id', channelId).order('score', { ascending: false }).limit(3);
      const names = top?.map(u => u.username).join(', ') || sender.username;
      const story = await askAI(
        `You are blazeguy, a creative Blaze.stream chat bot. Write a VERY short funny story (2-3 sentences max) featuring the given viewers as characters. Fun and stream-themed. No markdown.`,
        `Write a story featuring these viewers: ${names}`,
        130
      );
      await say(story ? `📖 ${story}` : `📖 Once upon a time, the chat was absolutely popping... and they all earned max loyalty points! ⚡`);
      break;
    }

    case '!aishoutout': {
      if (!arg) { await say(`Usage: !aishoutout @username`); return; }
      const target = arg.replace('@','');
      const { data: u } = await supabase.from('loyalty').select('*').eq('channel_id', channelId).eq('username', target).single();
      const ms   = u ? getMilestone(u.score) : MILESTONES[0];
      const context = u
        ? `They have ${u.score} loyalty points, sent ${u.msgs} messages, ${u.subs} subs, ${u.gifts} gifts. They are a "${ms.name}" tier member.`
        : `They are a new member of the community.`;
      const shoutout = await askAI(
        `You are blazeguy, an energetic Blaze.stream chat bot. Write a personalised hype shoutout for a viewer (2 sentences max, 30 words max). Use their stats to make it feel genuine. Mention their Blaze channel. No markdown.`,
        `Shoutout for @${target}. ${context}`,
        100
      );
      await say(shoutout ? `📣 ${shoutout} Check them out at blaze.stream/${target} ⚡` : `📣 Massive shoutout to @${target}! An amazing member of this community — go check them out! ⚡`);
      break;
    }

    default: {
      // Check custom commands in DB
      const { data: custom } = await supabase.from('custom_commands').select('response,uses,id').eq('channel_id', channelId).eq('trigger', cmd).single();
      if (custom) {
        await say(custom.response.replace('{user}', `@${sender.username}`).replace('{channel}', channelId));
        await supabase.from('custom_commands').update({ uses: (custom.uses || 0) + 1 }).eq('id', custom.id);
      }
    }
  }
}

// ── EVENT HANDLER ──
async function handleEvent(channelId, token, type, payload) {
  switch (type) {
    case 'channel.chat.message': {
      const { sender, message } = payload;

      // Handle !join and !leave FIRST before anything else
      const msgCmd = message.trim().split(/\s+/)[0].toLowerCase();
      console.log(`[${channelId.slice(0,8)}] MSG from @${sender.username}: ${message.slice(0,60)}`);

      if (msgCmd === '!join') { await handleJoin(sender); return; }
      if (msgCmd === '!leave') { await handleLeave(channelId, sender); return; }

      const settings = await getSettings(channelId);

      // Spam protection
      if (settings.spam_protection && isSpam(message)) {
        console.log(`[${channelId.slice(0,8)}] SPAM from @${sender.username}`);
        try {
          await fetch(`${API}/moderation/bans`, {
            method: 'POST', headers: blazeHeaders(token),
            body: JSON.stringify({ channelId, userId: sender.id })
          });
          await pushAlert(channelId, 'mod', `🛡 Spam bot @${sender.username} auto-banned.`, '🛡');
        } catch {}
        return;
      }

      // Phrase tracking
      if (settings.tracked_phrase && message.toLowerCase().includes(settings.tracked_phrase)) {
        await supabase.from('phrase_tracking').upsert({
          channel_id: channelId, phrase: settings.tracked_phrase,
          user_id: sender.id, username: sender.username,
          count: 1, last_at: new Date().toISOString()
        }, { onConflict: 'channel_id,phrase,user_id' });
        await supabase.rpc('increment_phrase_count', { p_channel_id: channelId, p_phrase: settings.tracked_phrase, p_user_id: sender.id }).catch(() => {});
      }

      // Poll vote detection
      const poll = settingsCache[channelId]?._poll;
      if (poll?.active && /^[1-9]$/.test(message.trim())) {
        const idx = parseInt(message.trim()) - 1;
        if (idx < poll.options.length && !poll.voters[sender.id]) {
          poll.votes[idx]++;
          poll.voters[sender.id] = true;
        }
      }

      // Trivia answer detection
      const trivia = activeTrivia[channelId];
      if (trivia && trivia.expiresAt > Date.now() && message.trim().toLowerCase() === trivia.answer) {
        delete activeTrivia[channelId];
        await upsertLoyalty(channelId, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'msg', trivia.reward);
        await sendChat(channelId, token, `🎉 @${sender.username} got it! The answer was "${trivia.answer}" — +${trivia.reward} pts!`);
        return;
      }

      await upsertLoyalty(channelId, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'msg', 1,
        { isSubscriber: sender.isSubscriber, isFollower: sender.isFollower });
      await pushAlert(channelId, 'chat', `${sender.displayName || sender.username}: ${message}`, '💬');

      if (message.startsWith('!')) handleCommand(channelId, token, message, sender);
      break;
    }
    case 'channel.follow': {
      const { follower } = payload;
      const settings = await getSettings(channelId);
      await upsertLoyalty(channelId, follower.id, follower.username, follower.displayName, follower.avatarUrl, 'follow', 1, { isFollower: true });
      await pushAlert(channelId, 'follow', `${follower.displayName || follower.username} followed! (+10 pts)`, '💙');
      if (settings.events_on) await sendChat(channelId, BOT_API_TOKEN,
        `💙 @${follower.username} just followed! Welcome to the community — you earned 10 loyalty pts! Type !loyalty to check your rank ⚡`);
      break;
    }
    case 'channel.unfollow': {
      const { follower } = payload;
      await supabase.rpc('decrement_score', { p_channel_id: channelId, p_user_id: follower.id, p_amount: 5 });
      await pushAlert(channelId, 'follow', `${follower.displayName || follower.username} unfollowed.`, '💔');
      break;
    }
    case 'channel.subscribe': {
      const { subscriber } = payload;
      const settings = await getSettings(channelId);
      await upsertLoyalty(channelId, subscriber.id, subscriber.username, subscriber.displayName, subscriber.avatarUrl, 'sub', 1, { isSubscriber: true });
      await pushAlert(channelId, 'sub', `${subscriber.displayName || subscriber.username} subscribed! (+50 pts)`, '💜');
      if (settings.events_on) await sendChat(channelId, BOT_API_TOKEN,
        `💜 @${subscriber.username} just subscribed! HUGE thank you! +50 loyalty pts added ┃ You are now a ${getMilestone(50).name} ${getMilestone(50).icon} ⚡`);
      break;
    }
    case 'channel.subscription.gift': {
      const { sender, giftCount } = payload;
      const settings = await getSettings(channelId);
      await upsertLoyalty(channelId, sender.id, sender.username, sender.displayName, sender.avatarUrl, 'gift', giftCount);
      await pushAlert(channelId, 'gift', `${sender.displayName || sender.username} gifted ${giftCount} sub${giftCount > 1 ? 's' : ''}!`, '🎁');
      if (settings.events_on) await sendChat(channelId, BOT_API_TOKEN,
        `🎁 @${sender.username} just gifted ${giftCount} sub${giftCount > 1 ? 's' : ''} to the community! LEGENDARY! +${SCORES.gift * giftCount} loyalty pts ┃ Thank you so much! ⚡🔥`);
      break;
    }
    case 'channel.vote': {
      const { voter } = payload;
      await upsertLoyalty(channelId, voter.id, voter.username, voter.displayName, voter.avatarUrl, 'vote', 1);
      await pushAlert(channelId, 'vote', `${voter.displayName || voter.username} voted! (+${SCORES.vote} pts)`, '⚡');
      break;
    }
    case 'channel.raid': {
      const { raider } = payload;
      await pushAlert(channelId, 'raid', `RAID from ${raider.displayName || raider.username}!`, '🚨');
      await sendChat(channelId, BOT_API_TOKEN,
        `🚨 RAID INCOMING! @${raider.displayName || raider.username} is raiding with their crew! Welcome everyone — type !commands to see what blazeguy can do! ⚡🔥`);
      break;
    }
    case 'channel.ban':      await pushAlert(channelId, 'mod', `${payload.bannedUser?.username} was banned.`, '🔨'); break;
    case 'channel.unban':    await pushAlert(channelId, 'mod', `${payload.unbannedUser?.username} was unbanned.`, '✅'); break;
    case 'channel.moderate': await pushAlert(channelId, 'mod', `Mod action: ${payload.action} on @${payload.targetUser?.username}`, '🛡'); break;
    case 'stream.online':    await pushAlert(channelId, 'stream', `Stream is LIVE! "${payload.title}"`, '🟢'); break;
    case 'stream.offline':   await pushAlert(channelId, 'stream', `Stream ended. Duration: ${Math.round((payload.durationSeconds || 0) / 60)} min.`, '🔴'); break;
  }
}

// ── CONNECT ONE CHANNEL ──
function connectChannel(channelId, token) {
  const existing = sockets[channelId];
  if (existing) {
    clearTimeout(existing.reconnectTimer);
    if (existing.socket) existing.socket.disconnect();
  }

  console.log(`[${channelId.slice(0,8)}] Connecting socket...`);
  const sock = io('https://blaze.stream', { path: '/ws', transports: ['websocket'] });
  sockets[channelId] = { socket: sock, sessionId: '', connected: false, reconnectTimer: null, token };

  sock.on('connect', () => console.log(`[${channelId.slice(0,8)}] Socket open`));

  sock.on('eventsub', async (msg) => {
    const { metadata, payload } = msg;
    if (metadata.messageType === 'session_welcome') {
      const sessionId = payload.sessionId;
      sockets[channelId].sessionId  = sessionId;
      sockets[channelId].connected  = true;
      await createSubscriptions(channelId, token, sessionId);
      await startTimedMessages(channelId, token);
      // Init settings cache
      await getSettings(channelId);
      // Mark connected in DB
      await supabase.from('channels').update({ connected: true, last_seen: new Date().toISOString() }).eq('id', channelId);
      console.log(`[${channelId.slice(0,8)}] LIVE — loyalty tracking active`);
      return;
    }
    handleEvent(channelId, BOT_API_TOKEN || token, metadata.subscriptionType, payload);
  });

  sock.on('disconnect', async (reason) => {
    if (sockets[channelId]) sockets[channelId].connected = false;
    await supabase.from('channels').update({ connected: false }).eq('id', channelId);
    console.log(`[${channelId.slice(0,8)}] Disconnected: ${reason}. Reconnect in 5s...`);
    sockets[channelId].reconnectTimer = setTimeout(() => connectChannel(channelId, token), 5000);
  });

  sock.on('connect_error', (e) => {
    if (sockets[channelId]) sockets[channelId].connected = false;
    console.error(`[${channelId.slice(0,8)}] Error: ${e.message}. Retry in 10s...`);
    sockets[channelId].reconnectTimer = setTimeout(() => connectChannel(channelId, token), 10000);
  });
}

// ── BOOT ──
async function bootChannels() {
  // 1 — Always connect bot's own channel to listen for !join / !leave
  console.log(`[BOT] Connecting own channel (${BOT_CHANNEL_ID.slice(0,8)}) for !join listener...`);
  connectChannel(BOT_CHANNEL_ID, BOT_TOKEN);

  // 2 — Reconnect all streamer channels saved in Supabase
  console.log('[BOT] Loading saved streamer channels...');
  const { data: saved, error } = await supabase
    .from('channels')
    .select('id, token')
    .not('token', 'is', null)
    .neq('id', BOT_CHANNEL_ID); // skip own channel, already connected above

  if (error) { console.error('[BOT] Supabase boot error:', error.message); return; }
  if (!saved?.length) { console.log('[BOT] No streamer channels yet — waiting for !join'); return; }

  console.log(`[BOT] Reconnecting ${saved.length} streamer channel(s)...`);
  for (const ch of saved) {
    connectChannel(ch.id, BOT_TOKEN); // always use bot token
  }

  // 3 — Resolve bot username from profile
  try {
    const res  = await fetch(`${API}/users/profile`, { headers: blazeHeaders(BOT_TOKEN) });
    const data = await res.json();
    BOT_USERNAME = data?.data?.username || data?.username || BOT_USERNAME;
    console.log(`[BOT] Running as @${BOT_USERNAME}`);
  } catch {}
}

// ── OAUTH PROXY — keeps client secret off the frontend ──
app.post('/oauth/auth-url', async (req, res) => {
  const { redirectUri } = req.body;
  if (!redirectUri) return res.status(400).json({ error: 'redirectUri required' });
  if (!CLIENT_SECRET) return res.status(500).json({ error: 'BLAZE_CLIENT_SECRET not set in Render env vars' });
  try {
    const r = await fetch('https://blaze.stream/bapi/oauth2/generate-auth-url', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        clientId:     CLIENT_ID,
        clientSecret: CLIENT_SECRET,
        redirectUri,
        scopes: ['users.read', 'offline.access', 'channel.moderate']
      })
    });
    const data = await r.json();
    console.log('[OAUTH] auth-url response:', r.status, JSON.stringify(data).slice(0,100));
    res.json(data);
  } catch (e) {
    console.error('[OAUTH] auth-url error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/oauth/token', async (req, res) => {
  const { code, codeVerifier, redirectUri } = req.body;
  if (!code) return res.status(400).json({ error: 'code required' });
  if (!CLIENT_SECRET) return res.status(500).json({ error: 'BLAZE_CLIENT_SECRET not set in Render env vars' });
  try {
    const r = await fetch('https://blaze.stream/bapi/oauth2/token', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        clientId:     CLIENT_ID,
        clientSecret: CLIENT_SECRET,
        code, codeVerifier, redirectUri,
        grantType: 'authorization_code'
      })
    });
    const data = await r.json();
    console.log('[OAUTH] token response:', r.status, JSON.stringify(data).slice(0,100));
    if (data.accessToken) {
      res.json({ ok: true, accessToken: data.accessToken, refreshToken: data.refreshToken, userId: data.userId, username: data.username, displayName: data.displayName, avatarUrl: data.avatarUrl, scopes: data.scopes });
    } else {
      res.json(data);
    }
  } catch (e) {
    console.error('[OAUTH] token error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/oauth/refresh', async (req, res) => {
  const { refreshToken } = req.body;
  if (!refreshToken) return res.status(400).json({ error: 'refreshToken required' });
  if (!CLIENT_SECRET) return res.status(500).json({ error: 'BLAZE_CLIENT_SECRET not set in Render env vars' });
  try {
    const r = await fetch('https://blaze.stream/bapi/oauth2/refresh', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ clientId: CLIENT_ID, clientSecret: CLIENT_SECRET, refreshToken })
    });
    const data = await r.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REST API ──
app.get('/', (req, res) => {
  const connected = Object.values(sockets).filter(s => s.connected).length;
  res.json({ name: 'blazeguy Bot', status: 'ok', channels: Object.keys(sockets).length, connected, uptime: Math.round((Date.now() - startedAt) / 1000) });
});

app.get('/health', (req, res) => {
  const connected = Object.values(sockets).filter(s => s.connected).length;
  res.json({ status: 'ok', channels: Object.keys(sockets).length, connected, uptime: Math.round((Date.now() - startedAt) / 1000) });
});

// Dashboard calls this after OAuth — saves token to Supabase, connects socket
app.post('/api/connect', async (req, res) => {
  const { token, channelId, username, displayName, avatarUrl } = req.body;
  if (!token || !channelId) return res.status(400).json({ error: 'token and channelId required' });

  // Save/update channel in Supabase
  await supabase.from('channels').upsert({
    id: channelId, token, username, display_name: displayName, avatar_url: avatarUrl,
    last_seen: new Date().toISOString()
  }, { onConflict: 'id' });

  connectChannel(channelId, token);
  res.json({ ok: true, message: 'Channel connected and saved!' });
});

// State for dashboard polling
app.get('/api/state', async (req, res) => {
  const { channelId } = req.query;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });

  const sock = sockets[channelId];

  const [loyaltyRes, alertsRes, statsRes] = await Promise.all([
    supabase.from('loyalty').select('*').eq('channel_id', channelId).order('score', { ascending: false }).limit(20),
    supabase.from('alerts').select('*').eq('channel_id', channelId).order('created_at', { ascending: false }).limit(50),
    supabase.from('loyalty').select('msgs.sum(),follows.sum(),subs.sum(),gifts.sum(),votes.sum()', { count: 'exact' }).eq('channel_id', channelId)
  ]);

  const agg = statsRes.data?.[0] || {};
  res.json({
    connected: sock?.connected || false,
    channelId,
    stats: {
      chatters: statsRes.count || 0,
      msgs:     agg['sum(msgs)']    || 0,
      follows:  agg['sum(follows)'] || 0,
      subs:     agg['sum(subs)']    || 0,
      gifts:    agg['sum(gifts)']   || 0,
      votes:    agg['sum(votes)']   || 0,
    },
    topUsers: loyaltyRes.data || [],
    alerts:   alertsRes.data  || [],
    uptime:   Math.round((Date.now() - startedAt) / 1000)
  });
});

// Send bot COMMANDS from dashboard — only ! commands allowed, no free chat
// This prevents the dashboard from being used as a chat client
app.post('/api/chat', async (req, res) => {
  const { channelId, message } = req.body;
  if (!channelId || !message) return res.status(400).json({ error: 'channelId and message required' });

  // Only allow ! commands — no free-form chat posting
  if (!message.trim().startsWith('!')) {
    return res.status(403).json({ error: 'Only bot commands (starting with !) can be sent from the dashboard.' });
  }

  const token = sockets[channelId]?.token || await getChannelToken(channelId);
  if (!token) return res.status(404).json({ error: 'Channel not connected' });

  // Run through bot command handler as if the owner typed it
  const fakeOwnerSender = { id: channelId, username: 'dashboard', displayName: 'Dashboard', avatarUrl: '', isSubscriber: false, isFollower: false };
  await handleCommand(channelId, token, message.trim(), fakeOwnerSender);
  res.json({ ok: true });
});

// Reset loyalty for a channel
app.post('/api/reset', async (req, res) => {
  const { channelId } = req.body;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });
  await supabase.from('loyalty').delete().eq('channel_id', channelId);
  await supabase.from('alerts').delete().eq('channel_id', channelId);
  res.json({ ok: true });
});

// List all channels
app.get('/api/channels', async (req, res) => {
  const { data } = await supabase.from('channels').select('id,username,display_name,connected,last_seen');
  res.json({ total: data?.length || 0, channels: data || [] });
});

// Debug — see all connected sockets and bot status
app.get('/api/debug', (req, res) => {
  res.json({
    bot_api_token_set:        !!BOT_API_TOKEN,
    bot_api_token_length:     BOT_API_TOKEN?.length || 0,
    bot_refresh_token_set:    !!BOT_REFRESH_TOKEN,
    bot_session_token_set:    !!BOT_SESSION_TOKEN,
    bot_session_token_length: BOT_SESSION_TOKEN?.length || 0,
    client_secret_set:        !!CLIENT_SECRET,
    gemini_set:               !!(process.env.GEMINI_API_KEY),
    bot_channel_id:           BOT_CHANNEL_ID,
    bot_username:             BOT_USERNAME,
    sockets: Object.entries(sockets).map(([id, s]) => ({
      channelId: id,
      connected: s.connected,
      sessionId: s.sessionId?.slice(0,12) || 'none'
    })),
    uptime: Math.round((Date.now() - startedAt) / 1000)
  });
});

// ── START ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`[BOT] blazeguy running on port ${PORT}`);
  console.log(`[BOT] API token length: ${BOT_API_TOKEN?.length || 0}`);
  console.log(`[BOT] Session token set: ${!!BOT_SESSION_TOKEN}`);
  console.log(`[BOT] Refresh token set: ${!!BOT_REFRESH_TOKEN}`);
  console.log(`[BOT] Groq set: ${!!(process.env.GEMINI_API_KEY)}`);
  if (!BOT_API_TOKEN)        console.error('[BOT] ⚠ BLAZE_BOT_TOKEN missing — chat will fail!');
  if (!BOT_SESSION_TOKEN)    console.warn('[BOT]  ⚠ BLAZE_SESSION_TOKEN missing — follow on !join will fail!');
  if (!BOT_REFRESH_TOKEN)    console.warn('[BOT]  ⚠ BLAZE_BOT_REFRESH_TOKEN missing — auto-refresh disabled!');
  if (!process.env.GEMINI_API_KEY) console.warn('[BOT] ⚠ GEMINI_API_KEY missing — AI commands disabled!');
  await bootChannels();
});
