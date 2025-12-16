const express = require('express');
const { Pool } = require('pg');
const { createClient } = require('redis');

const app = express();
app.use(express.json());

// --- CONFIGURATION ---

// 1. Connexion pour les ÉCRITURES (Point d'entrée HAProxy -> Primary)
const writePool = new Pool({
    user: 'app',
    host: 'localhost', // On tape sur le localhost car on lance le script hors docker
    database: 'appdb',
    password: 'app_pwd',
    port: 5439 // Port HAProxy
});

// 2. Connexion pour les LECTURES (Direct vers Replica)
const readPool = new Pool({
    user: 'app',
    host: 'localhost',
    database: 'appdb',
    password: 'app_pwd',
    port: 5433 // Port Replica
});

// 3. Client Redis
const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('Redis Client Error', err));

// --- LOGIQUE API ---

// POST: Créer un produit (Écriture -> HAProxy -> Primary)
app.post('/products', async (req, res) => {
    const { name, price_cents } = req.body;
    try {
        const result = await writePool.query(
            'INSERT INTO products(name, price_cents) VALUES($1, $2) RETURNING *',
            [name, price_cents]
        );
        res.status(201).json(result.rows[0]);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Erreur écriture DB" });
    }
});

// GET: Lire un produit (Cache Redis || Lecture -> Replica)
app.get('/products/:id', async (req, res) => {
    const { id } = req.params;
    const cacheKey = `product:${id}`;

    try {
        // A. Vérifier le Cache (Cache-Aside)
        // Note: On met un try/catch spécifique pour que l'API survive si Redis est down (Partie E1)
        try {
            const cachedData = await redisClient.get(cacheKey);
            if (cachedData) {
                console.log(`[CACHE HIT] Product ${id}`);
                return res.json(JSON.parse(cachedData));
            }
        } catch (redisErr) {
            console.warn("Redis indisponible, passage direct DB");
        }

        // B. Si Miss ou Redis Down : Lecture DB Replica
        console.log(`[CACHE MISS] Product ${id} -> Reading DB Replica`);
        const result = await readPool.query('SELECT * FROM products WHERE id = $1', [id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Produit non trouvé" });
        }

        const product = result.rows[0];

        // C. Mise en cache (avec TTL 60s)
        try {
            // EX = Expire en secondes
            await redisClient.set(cacheKey, JSON.stringify(product), { EX: 60 });
        } catch (redisErr) { /* Ignorer erreur écriture cache */ }

        res.json(product);

    } catch (err) {
        console.error(err);
        // Fallback: Si le réplica est mort, on pourrait essayer le primary ici (Partie E2)
        res.status(500).json({ error: "Erreur lecture DB" });
    }
});

// PUT: Mise à jour (Invalidation Cache + Écriture Primary)
app.put('/products/:id', async (req, res) => {
    const { id } = req.params;
    const { price_cents } = req.body;

    try {
        // 1. Update DB Primary
        await writePool.query(
            'UPDATE products SET price_cents = $1, updated_at = NOW() WHERE id = $2',
            [price_cents, id]
        );

        // 2. Invalidation Cache (Suppression de la clé)
        try {
            await redisClient.del(`product:${id}`);
            console.log(`[CACHE INVALIDATE] Product ${id}`);
        } catch (e) { console.error("Erreur Redis del"); }

        res.json({ status: "Updated" });
    } catch (err) {
        res.status(500).json({ error: "Erreur update" });
    }
});

// Démarrage
(async () => {
    await redisClient.connect();
    app.listen(3000, () => {
        console.log('API running on port 3000');
    });
})();