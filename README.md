# 🤖 Mega Video Compressor — Telegram Bot

Mega.nz video/image links compress karke wapas Mega pe upload karne wala free Telegram Bot.
GitHub Actions pe bilkul **free** chalta hai — koi server kharidne ki zaroorat nahi!

---

## ✨ Features

- 🎬 **Video Compression** — H.265 (HEVC) CRF-18, visually lossless quality
- 🖼️ **Image Optimization** — Pillow se lossless/near-lossless compression
- 📊 **Stats** — Original vs Compressed size aur % savings dikhata hai
- 🔄 **24/7 Free** — GitHub Actions pe automatic restart every 5 hours
- ☁️ **Mega.nz** — Download aur upload dono Mega se

---

## 🚀 Setup Guide (Step by Step)

### Step 1: Telegram Bot banao

1. Telegram pe **[@BotFather](https://t.me/BotFather)** ko message karo
2. `/newbot` command bhejo
3. Bot ka naam aur username dalo
4. **Token copy karo** — kuch aisa dikhega:
   ```
   1234567890:ABCDEFghijklmnopqrstuvwxyz
   ```

---

### Step 2: GitHub Repository banao

1. **[github.com](https://github.com)** pe naya repository banao
2. Ye saare files upload karo:
   ```
   your-repo/
   ├── bot.py
   ├── requirements.txt
   └── .github/
       └── workflows/
           └── bot.yml
   ```

---

### Step 3: Secrets add karo (IMPORTANT)

GitHub repo mein jao → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Teen secrets add karo:

| Secret Name       | Value                          |
|-------------------|-------------------------------|
| `TELEGRAM_TOKEN`  | BotFather se mila token        |
| `MEGA_EMAIL`      | Tumhara Mega.nz email          |
| `MEGA_PASSWORD`   | Tumhara Mega.nz password       |

---

### Step 4: Bot start karo

1. GitHub repo mein **Actions** tab kholो
2. **"Mega Compressor Bot"** workflow dhundho
3. **"Run workflow"** button dabao
4. ✅ Bot shuru ho gaya!

---

## 📱 Bot Use Kaise Karein

1. Telegram pe apna bot kholо
2. `/start` bhejo
3. Koi bhi **Mega.nz link** bhejo
4. Bot process karega aur compressed file ka link wapas bhejega!

```
Tum:  https://mega.nz/file/XXXXXX#YYYYYY
Bot:  ✅ Kaam ho gaya!
      • Pehle:  850.0 MB
      • Baad:   320.5 MB
      • Saved:  62.3% 🔥
      🔗 https://mega.nz/file/ZZZZZZ#WWWWWW
```

---

## ⚙️ Compression Settings

| Setting  | Value  | Meaning                             |
|----------|--------|-------------------------------------|
| Codec    | H.265  | Latest, best compression            |
| CRF      | 18     | Visually lossless (0=perfect)       |
| Preset   | slow   | Better compression (thoda slow)     |
| Audio    | copy   | Audio unchanged, no quality loss    |

---

## 💾 GitHub Actions — Free Limits

| Resource    | Limit                          |
|-------------|-------------------------------|
| Storage     | 14 GB per job                  |
| RAM         | 7 GB                           |
| CPU         | 2 cores                        |
| Max runtime | 6 hours per job                |
| Free minutes| 2,000 min/month (public repo)  |

> **Tip:** Public repo banao — unlimited free minutes milenge!

---

## ❓ FAQ

**Q: Bot band ho jata hai?**
A: GitHub Actions max 6 hours run karta hai. Workflow har 5 ghante mein auto-restart hota hai.

**Q: Bahut badi file (5GB+) support hoti hai?**
A: Runner pe 14GB space hai, lekin processing time zyada lagta hai.

**Q: Mega password safe hai?**
A: Haan, GitHub Secrets encrypted rehte hain, koi dekh nahi sakta.

---

## 📄 License

MIT — Free use karo, modify karo!
