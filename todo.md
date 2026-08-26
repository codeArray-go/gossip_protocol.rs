## 1. The "Localhost Illusion" (Packet Loss Handling)

Localhost par OS packets drop nahi karta, isliye aapka 10MB data ekdum perfect order mein bina loss ke receive ho gaya. Lekin real Wi-Fi ya internet par **UDP packets guarantee ke sath drop honge**.
Agar 10MB file ke 8,000 chunks hain aur sirf 1 chunk drop ho gaya, toh poori file corrupt ho jayegi.

* **Agla Task:** Aapko ek **NACK (Negative Acknowledgement)** ya **ACK (Acknowledgement)** system banana padega.
* **Kaise kaam karega:** Jab receiver ko pata chale ki `total_chunks` 8000 hain, par uske `HashMap` mein sirf 7999 chunks aaye hain, toh woh sender ko ek chota sa UDP message bhejega: *"Bhai, msg_id XYZ ka chunk number 405 missing hai, wapas bhej."*

## 2. NAT Traversal & Hole Punching (Going beyond Local)

Jab aap alag-alag networks par do doston ke beech yeh P2P chalayenge, toh unke routers (NAT - Network Address Translation) direct UDP packets block kar denge. Yahan relay aur traversal ki zaroorat padti hai.

* **UDP Hole Punching (STUN):** Yeh ek technique hai jahan dono peers ek doosre ke router mein "hole punch" karte hain taaki direct connection ban sake (bina relay ke). Yeh fast aur free hai.
* **Relay Server (TURN):** Agar Hole Punching fail ho jaye (jo kuch strict routers mein hota hai), tab aakhiri raste ke taur par ek Relay server ka use kiya jata hai jo dono ka data pass karta hai.

## 3. Peer Discovery (No more Manual IPs)

Abhi shayad aap terminal mein manual IP aur port daal kar connect kar rahe hain. Ek real P2P system doosre peers ko khud dhoondhta hai.

* **LAN ke liye (mDNS):** Aap Multicast DNS implement kar sakte hain. Isse jaise hi aap app run karenge, woh local network par baaki sabhi nodes ko khud dhoondh lega.
* **Internet ke liye (Bootstrap Nodes):** Ek hardcoded server (ya IP) jahan naye peers connect karke doosre active peers ki list maangte hain, aur phir direct connect ho jate hain.

