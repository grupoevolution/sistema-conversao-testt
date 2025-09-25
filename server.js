const express = require('express');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs').promises;
const app = express();

// ============ CONFIGURAÇÕES ============
const EVOLUTION_BASE_URL = process.env.EVOLUTION_BASE_URL || 'https://evo.flowzap.fun';
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY || 'SUA_API_KEY_AQUI';
const PIX_TIMEOUT = 7 * 60 * 1000; // 7 minutos
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data', 'funnels.json');
const CONVERSATIONS_FILE = path.join(__dirname, 'data', 'conversations.json');
const CONTACTS_FILE = path.join(__dirname, 'data', 'contacts.json'); // ✅ NOVO
const DAILY_STATS_FILE = path.join(__dirname, 'data', 'daily-stats.json'); // ✅ NOVO

// Mapeamento dos produtos Kirvano
const PRODUCT_MAPPING = {
    '5c1f6390-8999-4740-b16f-51380e1097e4': 'CS',
    '0f393085-4960-4c71-9efe-faee8ba51d3f': 'CS',
    'e2282b4c-878c-4bcd-becb-1977dfd6d2b8': 'CS',
    '5288799c-d8e3-48ce-a91d-587814acdee5': 'FAB'
};

// Instâncias Evolution (fallback sequencial)
const INSTANCES = ['GABY01', 'GABY02', 'GABY03', 'GABY04', 'GABY05', 'GABY06', 'GABY07', 'GABY08', 'GABY09'];

// ============ ARMAZENAMENTO EM MEMÓRIA ============
let conversations = new Map();
let idempotencyCache = new Map();
let stickyInstances = new Map();
let pixTimeouts = new Map();
let logs = [];
let funis = new Map();
let lastSuccessfulInstanceIndex = -1;
let capturedContacts = new Map(); // ✅ NOVO: Contatos capturados por instância
let dailyStats = { // ✅ NOVO: Estatísticas diárias
    date: new Date().toISOString().split('T')[0],
    firstMessages: new Set(),
    totalEvents: 0
};

// ✅ FUNIS PADRÃO COM LÓGICA DE CAPTURA DE CONTATOS CORRIGIDA
const defaultFunnels = {
    'CS_APROVADA': {
        id: 'CS_APROVADA',
        name: 'CS - Compra Aprovada',
        steps: [
            {
                id: 'step_1',
                type: 'text',
                text: 'Parabéns! Seu pedido foi aprovado. Bem-vindo ao CS!',
                waitForReply: true,
                timeoutMinutes: 60,
                nextOnReply: 1, // ✅ CORREÇÃO: vai para passo 1 (índice)
                nextOnTimeout: 2
            },
            {
                id: 'step_2',
                type: 'text',
                text: 'Obrigado pela resposta! Agora me confirma se recebeu o acesso ao curso por email?',
                waitForReply: true,
                captureContact: true, // ✅ NOVO: Marcar para capturar contato
                timeoutMinutes: 30,
                nextOnReply: 2, // ✅ CORREÇÃO: vai para passo 2 (índice)
                nextOnTimeout: 2
            },
            {
                id: 'step_3',
                type: 'text',
                text: 'Perfeito! Lembre-se de acessar nossa plataforma. Qualquer dúvida, estamos aqui!',
                waitForReply: false
            }
        ]
    },
    'CS_PIX': {
        id: 'CS_PIX',
        name: 'CS - PIX Pendente',
        steps: [
            {
                id: 'step_1',
                type: 'text',
                text: 'Seu PIX foi gerado! Aguardamos o pagamento para liberar o acesso ao CS.',
                waitForReply: true,
                timeoutMinutes: 10,
                nextOnReply: 1, // ✅ CORREÇÃO: vai para passo 1
                nextOnTimeout: 2
            },
            {
                id: 'step_2',
                type: 'text',
                text: 'Obrigado pelo contato! Me confirma que está com dificuldades no pagamento?',
                waitForReply: true,
                captureContact: true, // ✅ NOVO: Capturar contato
                timeoutMinutes: 15,
                nextOnReply: 2, // ✅ CORREÇÃO: vai para passo 2
                nextOnTimeout: 2
            },
            {
                id: 'step_3',
                type: 'text',
                text: 'PIX vencido! Entre em contato conosco para gerar um novo.',
                waitForReply: false
            }
        ]
    },
    'FAB_APROVADA': {
        id: 'FAB_APROVADA',
        name: 'FAB - Compra Aprovada',
        steps: [
            {
                id: 'step_1',
                type: 'text',
                text: 'Parabéns! Seu pedido FAB foi aprovado. Prepare-se para a transformação!',
                waitForReply: true,
                timeoutMinutes: 60,
                nextOnReply: 1, // ✅ CORREÇÃO: vai para passo 1
                nextOnTimeout: 2
            },
            {
                id: 'step_2',
                type: 'text',
                text: 'Que bom que respondeu! Já acessou a área de membros do FAB?',
                waitForReply: true,
                captureContact: true, // ✅ NOVO: Capturar contato
                timeoutMinutes: 30,
                nextOnReply: 2, // ✅ CORREÇÃO: vai para passo 2
                nextOnTimeout: 2
            },
            {
                id: 'step_3',
                type: 'text',
                text: 'Acesse nossa área de membros e comece sua transformação hoje mesmo!',
                waitForReply: false
            }
        ]
    },
    'FAB_PIX': {
        id: 'FAB_PIX',
        name: 'FAB - PIX Pendente',
        steps: [
            {
                id: 'step_1',
                type: 'text',
                text: 'Seu PIX FAB foi gerado! Aguardamos o pagamento para iniciar sua transformação.',
                waitForReply: true,
                timeoutMinutes: 10,
                nextOnReply: 1, // ✅ CORREÇÃO: vai para passo 1
                nextOnTimeout: 2
            },
            {
                id: 'step_2',
                type: 'text',
                text: 'Obrigado pelo contato! Está com alguma dúvida sobre o pagamento?',
                waitForReply: true,
                captureContact: true, // ✅ NOVO: Capturar contato
                timeoutMinutes: 15,
                nextOnReply: 2, // ✅ CORREÇÃO: vai para passo 2
                nextOnTimeout: 2
            },
            {
                id: 'step_3',
                type: 'text',
                text: 'PIX vencido! Entre em contato para gerar um novo e não perder essa oportunidade.',
                waitForReply: false
            }
        ]
    }
};

// ============ PERSISTÊNCIA DE DADOS ============

async function ensureDataDir() {
    try {
        await fs.mkdir(path.join(__dirname, 'data'), { recursive: true });
    } catch (error) {
        console.log('Pasta data já existe ou erro ao criar:', error.message);
    }
}

async function saveFunnelsToFile() {
    try {
        await ensureDataDir();
        const funnelsArray = Array.from(funis.values());
        await fs.writeFile(DATA_FILE, JSON.stringify(funnelsArray, null, 2));
        addLog('DATA_SAVE', 'Funis salvos em arquivo: ' + funnelsArray.length + ' funis');
    } catch (error) {
        addLog('DATA_SAVE_ERROR', 'Erro ao salvar funis: ' + error.message);
    }
}

async function loadFunnelsFromFile() {
    try {
        const data = await fs.readFile(DATA_FILE, 'utf8');
        const funnelsArray = JSON.parse(data);
        
        funis.clear();
        
        funnelsArray.forEach(funnel => {
            funis.set(funnel.id, funnel);
        });
        
        addLog('DATA_LOAD', 'Funis carregados do arquivo: ' + funnelsArray.length + ' funis');
        return true;
    } catch (error) {
        addLog('DATA_LOAD_ERROR', 'Erro ao carregar funis (usando padrões): ' + error.message);
        return false;
    }
}

async function saveConversationsToFile() {
    try {
        await ensureDataDir();
        const conversationsArray = Array.from(conversations.entries()).map(([key, value]) => ({
            remoteJid: key,
            ...value,
            createdAt: value.createdAt.toISOString(),
            lastSystemMessage: value.lastSystemMessage ? value.lastSystemMessage.toISOString() : null,
            lastReply: value.lastReply ? value.lastReply.toISOString() : null
        }));
        
        await fs.writeFile(CONVERSATIONS_FILE, JSON.stringify({
            conversations: conversationsArray,
            stickyInstances: Array.from(stickyInstances.entries())
        }, null, 2));
        
        addLog('DATA_SAVE', 'Conversas salvas: ' + conversationsArray.length + ' conversas');
    } catch (error) {
        addLog('DATA_SAVE_ERROR', 'Erro ao salvar conversas: ' + error.message);
    }
}

async function loadConversationsFromFile() {
    try {
        const data = await fs.readFile(CONVERSATIONS_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        conversations.clear();
        parsed.conversations.forEach(conv => {
            const conversation = {
                ...conv,
                createdAt: new Date(conv.createdAt),
                lastSystemMessage: conv.lastSystemMessage ? new Date(conv.lastSystemMessage) : null,
                lastReply: conv.lastReply ? new Date(conv.lastReply) : null
            };
            conversations.set(conv.remoteJid, conversation);
        });
        
        stickyInstances.clear();
        parsed.stickyInstances.forEach(([key, value]) => {
            stickyInstances.set(key, value);
        });
        
        addLog('DATA_LOAD', 'Conversas carregadas: ' + parsed.conversations.length + ' conversas');
        return true;
    } catch (error) {
        addLog('DATA_LOAD_ERROR', 'Nenhuma conversa anterior encontrada: ' + error.message);
        return false;
    }
}

// ✅ NOVAS FUNÇÕES DE PERSISTÊNCIA PARA CONTATOS E ESTATÍSTICAS

async function saveContactsToFile() {
    try {
        await ensureDataDir();
        const contactsData = {
            byInstance: Object.fromEntries(capturedContacts),
            lastUpdate: new Date().toISOString()
        };
        
        await fs.writeFile(CONTACTS_FILE, JSON.stringify(contactsData, null, 2));
        addLog('CONTACTS_SAVE', 'Contatos salvos: ' + Array.from(capturedContacts.values()).reduce((total, contacts) => total + contacts.length, 0) + ' contatos');
    } catch (error) {
        addLog('CONTACTS_SAVE_ERROR', 'Erro ao salvar contatos: ' + error.message);
    }
}

async function loadContactsFromFile() {
    try {
        const data = await fs.readFile(CONTACTS_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        capturedContacts.clear();
        Object.entries(parsed.byInstance || {}).forEach(([instance, contacts]) => {
            capturedContacts.set(instance, contacts);
        });
        
        addLog('CONTACTS_LOAD', 'Contatos carregados');
        return true;
    } catch (error) {
        addLog('CONTACTS_LOAD_ERROR', 'Nenhum contato anterior encontrado: ' + error.message);
        return false;
    }
}

async function saveDailyStatsToFile() {
    try {
        await ensureDataDir();
        const statsData = {
            ...dailyStats,
            firstMessages: Array.from(dailyStats.firstMessages)
        };
        
        await fs.writeFile(DAILY_STATS_FILE, JSON.stringify(statsData, null, 2));
    } catch (error) {
        addLog('STATS_SAVE_ERROR', 'Erro ao salvar estatísticas: ' + error.message);
    }
}

async function loadDailyStatsFromFile() {
    try {
        const data = await fs.readFile(DAILY_STATS_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        const today = new Date().toISOString().split('T')[0];
        
        if (parsed.date === today) {
            dailyStats = {
                ...parsed,
                firstMessages: new Set(parsed.firstMessages || [])
            };
        } else {
            // Novo dia, resetar estatísticas
            dailyStats = {
                date: today,
                firstMessages: new Set(),
                totalEvents: 0
            };
        }
        
        return true;
    } catch (error) {
        // Arquivo não existe, manter padrão
        return false;
    }
}

// Auto-save periódico
setInterval(async () => {
    await saveFunnelsToFile();
    await saveConversationsToFile();
    await saveContactsToFile(); // ✅ NOVO
    await saveDailyStatsToFile(); // ✅ NOVO
}, 30000);

// Inicializar funis padrão
Object.values(defaultFunnels).forEach(funnel => {
    funis.set(funnel.id, funnel);
});

// ============ MIDDLEWARES ============
app.use(express.json());
app.use(express.static('public'));

// ============ FUNÇÕES AUXILIARES MELHORADAS ============

// ✅ NORMALIZAÇÃO ROBUSTA DE TELEFONE
function normalizePhone(phone) {
    if (!phone) return '';
    
    // Remove todos os caracteres não numéricos
    let cleaned = phone.replace(/\D/g, '');
    
    // Remove código +55 se presente
    if (cleaned.startsWith('55')) {
        cleaned = cleaned.substring(2);
    }
    
    // ✅ NORMALIZAÇÃO ROBUSTA PARA NÚMEROS BRASILEIROS
    
    // Se tem 10 dígitos (DDD + 8 dígitos), adicionar 9
    if (cleaned.length === 10) {
        const ddd = cleaned.substring(0, 2);
        const numero = cleaned.substring(2);
        cleaned = ddd + '9' + numero; // Adiciona o 9
    }
    
    // Se tem 11 dígitos, verificar se tem 9 após o DDD
    if (cleaned.length === 11) {
        const ddd = cleaned.substring(0, 2);
        const primeiroDigito = cleaned.substring(2, 3);
        
        // Se o primeiro dígito após DDD não é 9, adicionar 9
        if (primeiroDigito !== '9') {
            const numero = cleaned.substring(2);
            cleaned = ddd + '9' + numero;
        }
    }
    
    // Garantir que tem exatamente 11 dígitos no final
    if (cleaned.length === 11) {
        cleaned = '55' + cleaned; // Adicionar código do país
    } else if (cleaned.length === 13 && cleaned.startsWith('55')) {
        // Já tem 55 + 11 dígitos, está correto
    } else {
        // Formato não reconhecido, tentar com código do país
        if (!cleaned.startsWith('55')) {
            cleaned = '55' + cleaned;
        }
    }
    
    addLog('PHONE_NORMALIZE', 'Número normalizado', { 
        input: phone, 
        output: cleaned,
        length: cleaned.length
    });
    
    return cleaned;
}

function phoneToRemoteJid(phone) {
    const normalized = normalizePhone(phone);
    return normalized + '@s.whatsapp.net';
}

// ✅ BUSCA FLEXÍVEL ROBUSTA DE CONVERSA
function findConversationByPhone(phone) {
    const normalized = normalizePhone(phone);
    const remoteJid = normalized + '@s.whatsapp.net';
    
    // Tentar encontrar conversa com número exato
    if (conversations.has(remoteJid)) {
        addLog('CONVERSATION_FOUND_EXACT', 'Conversa encontrada com número exato', { remoteJid });
        return conversations.get(remoteJid);
    }
    
    // ✅ BUSCA FLEXÍVEL: Criar variações do número
    const phoneOnly = normalized.replace('55', ''); // Remove código do país
    const variations = [
        normalized + '@s.whatsapp.net',                    // 5575981734444@s.whatsapp.net
        '55' + phoneOnly + '@s.whatsapp.net',             // Com código país
        phoneOnly + '@s.whatsapp.net',                    // Sem código país: 75981734444@s.whatsapp.net
    ];
    
    // Se tem 11 dígitos, criar variação sem 9
    if (phoneOnly.length === 11 && phoneOnly.charAt(2) === '9') {
        const ddd = phoneOnly.substring(0, 2);
        const numeroSem9 = phoneOnly.substring(3);
        variations.push(ddd + numeroSem9 + '@s.whatsapp.net');           // 7581734444@s.whatsapp.net
        variations.push('55' + ddd + numeroSem9 + '@s.whatsapp.net');   // 557581734444@s.whatsapp.net
    }
    
    // Buscar em todas as variações
    for (const variation of variations) {
        if (conversations.has(variation)) {
            addLog('CONVERSATION_FOUND_VARIATION', 'Conversa encontrada com variação', { 
                searched: remoteJid,
                found: variation,
                variations: variations
            });
            
            // ✅ IMPORTANTE: Atualizar a chave da conversa para o formato normalizado
            const conversation = conversations.get(variation);
            conversations.delete(variation); // Remove entrada antiga
            conversations.set(remoteJid, conversation); // Adiciona com chave normalizada
            
            // Atualizar sticky instance também
            if (stickyInstances.has(variation)) {
                const instance = stickyInstances.get(variation);
                stickyInstances.delete(variation);
                stickyInstances.set(remoteJid, instance);
            }
            
            return conversation;
        }
    }
    
    addLog('CONVERSATION_NOT_FOUND', 'Nenhuma conversa encontrada', { 
        searched: remoteJid,
        variations: variations,
        existingConversations: Array.from(conversations.keys())
    });
    
    return null;
}

function extractMessageText(message) {
    if (!message) return '';
    if (message.conversation) return message.conversation;
    if (message.extendedTextMessage?.text) return message.extendedTextMessage.text;
    if (message.imageMessage?.caption) return message.imageMessage.caption;
    if (message.videoMessage?.caption) return message.videoMessage.caption;
    if (message.buttonsResponseMessage?.selectedDisplayText) 
        return message.buttonsResponseMessage.selectedDisplayText;
    if (message.listResponseMessage?.singleSelectReply?.selectedRowId)
        return message.listResponseMessage.singleSelectReply.selectedRowId;
    if (message.templateButtonReplyMessage?.selectedId)
        return message.templateButtonReplyMessage.selectedId;
    return '';
}

function checkIdempotency(key, ttl = 5 * 60 * 1000) {
    const now = Date.now();
    for (const [k, timestamp] of idempotencyCache.entries()) {
        if (now - timestamp > ttl) {
            idempotencyCache.delete(k);
        }
    }
    if (idempotencyCache.has(key)) return true;
    idempotencyCache.set(key, now);
    return false;
}

function addLog(type, message, data = null) {
    const log = {
        id: Date.now() + Math.random(),
        timestamp: new Date(),
        type,
        message,
        data
    };
    logs.unshift(log);
    if (logs.length > 1000) {
        logs = logs.slice(0, 1000);
    }
    console.log('[' + log.timestamp.toISOString() + '] ' + type + ': ' + message);
}

// ✅ NOVA FUNÇÃO: Capturar contato para lista
function captureContact(remoteJid, instanceName, customerName) {
    const phoneNumber = remoteJid.replace('@s.whatsapp.net', '');
    const today = new Date().toISOString().split('T')[0].split('-').reverse().join('/'); // Formato DD/MM
    
    if (!capturedContacts.has(instanceName)) {
        capturedContacts.set(instanceName, []);
    }
    
    const contacts = capturedContacts.get(instanceName);
    
    // Verificar se já não foi capturado
    const alreadyCaptured = contacts.some(contact => contact.phone === phoneNumber);
    
    if (!alreadyCaptured) {
        contacts.push({
            date: today,
            phone: phoneNumber,
            name: customerName,
            capturedAt: new Date().toISOString()
        });
        
        addLog('CONTACT_CAPTURED', `Contato capturado para ${instanceName}`, {
            phone: phoneNumber,
            name: customerName,
            instance: instanceName
        });
        
        // Salvar imediatamente
        saveContactsToFile();
    }
}

// ============ EVOLUTION API ADAPTER ============
async function sendToEvolution(instanceName, endpoint, payload) {
    const url = EVOLUTION_BASE_URL + endpoint + '/' + instanceName;
    try {
        const response = await axios.post(url, payload, {
            headers: {
                'Content-Type': 'application/json',
                'apikey': EVOLUTION_API_KEY
            },
            timeout: 15000
        });
        return { ok: true, data: response.data };
    } catch (error) {
        return { 
            ok: false, 
            error: error.response?.data || error.message,
            status: error.response?.status
        };
    }
}

async function sendText(remoteJid, text, clientMessageId, instanceName) {
    const payload = {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        text: text
    };
    return await sendToEvolution(instanceName, '/message/sendText', payload);
}

async function sendImage(remoteJid, imageUrl, caption, clientMessageId, instanceName) {
    const payload = {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        mediatype: 'image',
        media: imageUrl,
        caption: caption || ''
    };
    return await sendToEvolution(instanceName, '/message/sendMedia', payload);
}

async function sendVideo(remoteJid, videoUrl, caption, clientMessageId, instanceName) {
    const payload = {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        mediatype: 'video',
        media: videoUrl,
        caption: caption || ''
    };
    return await sendToEvolution(instanceName, '/message/sendMedia', payload);
}

async function sendAudio(remoteJid, audioUrl, clientMessageId, instanceName) {
    const payload = {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        mediatype: 'audio',
        media: audioUrl
    };
    return await sendToEvolution(instanceName, '/message/sendMedia', payload);
}

// ============ ENVIO COM STICKY INSTANCE MELHORADO ============

// ✅ STICKY INSTANCE ROBUSTO COM ROUND-ROBIN CORRIGIDO
async function sendWithFallback(remoteJid, type, text, mediaUrl, isFirstMessage = false) {
    const clientMessageId = uuidv4();
    let instancesToTry = [...INSTANCES];
    
    // ✅ STICKY INSTANCE: Priorizar instância fixa do cliente
    const stickyInstance = stickyInstances.get(remoteJid);
    if (stickyInstance && !isFirstMessage) {
        // Mover sticky instance para primeiro da fila
        instancesToTry = [stickyInstance, ...INSTANCES.filter(i => i !== stickyInstance)];
        
        addLog('STICKY_INSTANCE_USED', `Usando instância fixa ${stickyInstance} para ${remoteJid}`, {
            stickyInstance,
            isFirstMessage
        });
    } else if (isFirstMessage) {
        // ✅ CORREÇÃO: Round-robin baseado na última instância que funcionou
        const nextInstanceIndex = (lastSuccessfulInstanceIndex + 1) % INSTANCES.length;
        const primaryInstance = INSTANCES[nextInstanceIndex];
        
        // Reorganizar lista começando da próxima instância na fila
        instancesToTry = [
            ...INSTANCES.slice(nextInstanceIndex),
            ...INSTANCES.slice(0, nextInstanceIndex)
        ];
        
        addLog('INSTANCE_DISTRIBUTION', `Nova conversa distribuída para ${primaryInstance} (índice ${nextInstanceIndex})`, { 
            remoteJid,
            primaryInstance,
            lastSuccessfulIndex: lastSuccessfulInstanceIndex,
            nextIndex: nextInstanceIndex,
            fallbackOrder: instancesToTry.slice(0, 3) // Mostrar apenas primeiros 3 para logs
        });
    }
    
    let lastError = null;
    let successfulInstanceIndex = -1;
    
    for (let i = 0; i < instancesToTry.length; i++) {
        const instanceName = instancesToTry[i];
        try {
            addLog('SEND_ATTEMPT', `Tentando ${instanceName} para ${remoteJid} (tentativa ${i + 1})`, { 
                type, 
                clientMessageId,
                isFirstMessage,
                stickyUsed: instanceName === stickyInstance
            });
            
            let result;
            
            if (type === 'text') {
                result = await sendText(remoteJid, text, clientMessageId, instanceName);
            } else if (type === 'image') {
                result = await sendImage(remoteJid, mediaUrl, '', clientMessageId, instanceName);
            } else if (type === 'image+text') {
                result = await sendImage(remoteJid, mediaUrl, text, clientMessageId, instanceName);
            } else if (type === 'video') {
                result = await sendVideo(remoteJid, mediaUrl, '', clientMessageId, instanceName);
            } else if (type === 'video+text') {
                result = await sendVideo(remoteJid, mediaUrl, text, clientMessageId, instanceName);
            } else if (type === 'audio') {
                result = await sendAudio(remoteJid, mediaUrl, clientMessageId, instanceName);
            }
            
            if (result && result.ok) {
                // ✅ CRITICAL: Sempre definir/manter sticky instance
                stickyInstances.set(remoteJid, instanceName);
                
                // ✅ CORREÇÃO: Atualizar índice da última instância bem-sucedida
                if (isFirstMessage) {
                    successfulInstanceIndex = INSTANCES.indexOf(instanceName);
                    lastSuccessfulInstanceIndex = successfulInstanceIndex;
                }
                
                // ✅ NOVO: Contar primeira mensagem para estatísticas
                if (isFirstMessage) {
                    dailyStats.firstMessages.add(remoteJid);
                }
                
                addLog('SEND_SUCCESS', `Mensagem enviada com sucesso via ${instanceName}`, { 
                    remoteJid, 
                    type,
                    isFirstMessage,
                    instanceIndex: successfulInstanceIndex,
                    nextWillStartFrom: isFirstMessage ? INSTANCES[(successfulInstanceIndex + 1) % INSTANCES.length] : 'N/A',
                    stickyInstanceSet: instanceName
                });
                
                return { success: true, instanceName };
            } else {
                lastError = result.error;
                addLog('SEND_FAILED', `${instanceName} falhou: ${JSON.stringify(lastError)}`, { remoteJid, type });
            }
        } catch (error) {
            lastError = error.message;
            addLog('SEND_ERROR', `${instanceName} erro: ${error.message}`, { remoteJid, type });
        }
    }
    
    addLog('SEND_ALL_FAILED', `Todas as instâncias falharam para ${remoteJid}`, { 
        lastError,
        triedInstances: instancesToTry.length
    });
    return { success: false, error: lastError };
}

// ============ ORQUESTRAÇÃO DE FUNIS ============
async function startFunnel(remoteJid, funnelId, orderCode, customerName, productType, amount) {
    // ✅ NOVO: Cancelar funil PIX existente se for venda aprovada
    if (funnelId.includes('APROVADA')) {
        await cancelPixFunnel(remoteJid, 'PAYMENT_APPROVED');
    }
    
    const conversation = {
        remoteJid,
        funnelId,
        stepIndex: 0,
        orderCode,
        customerName,
        productType,
        amount,
        waiting_for_response: false,
        createdAt: new Date(),
        lastSystemMessage: null,
        lastReply: null
    };
    
    conversations.set(remoteJid, conversation);
    addLog('FUNNEL_START', `Iniciando funil ${funnelId} para ${remoteJid}`, { orderCode, productType });
    await sendStep(remoteJid);
}

// ✅ NOVA FUNÇÃO: Cancelar funil PIX quando pagamento aprovado
async function cancelPixFunnel(remoteJid, reason) {
    const conversation = conversations.get(remoteJid);
    
    if (!conversation) return;
    
    // Verificar se é funil PIX
    if (!conversation.funnelId.includes('PIX')) return;
    
    addLog('PIX_FUNNEL_CANCEL', `Cancelando funil PIX para ${remoteJid}`, { 
        funnelId: conversation.funnelId, 
        currentStep: conversation.stepIndex,
        reason 
    });
    
    // Cancelar timeout PIX se existir
    const pixTimeout = pixTimeouts.get(remoteJid);
    if (pixTimeout) {
        clearTimeout(pixTimeout.timeout);
        pixTimeouts.delete(remoteJid);
        addLog('PIX_TIMEOUT_CANCELED', `Timeout PIX cancelado para ${remoteJid}`, { reason });
    }
    
    // Marcar conversa como cancelada mas não removê-la (para histórico)
    conversation.waiting_for_response = false;
    conversation.canceled = true;
    conversation.canceledAt = new Date();
    conversation.cancelReason = reason;
    
    conversations.set(remoteJid, conversation);
}

async function sendStep(remoteJid) {
    const conversation = conversations.get(remoteJid);
    if (!conversation) return;
    
    // ✅ NOVO: Verificar se conversa foi cancelada
    if (conversation.canceled) {
        addLog('STEP_CANCELED', `Tentativa de envio em conversa cancelada: ${conversation.funnelId}`, { remoteJid });
        return;
    }
    
    const funnel = funis.get(conversation.funnelId);
    if (!funnel) return;
    
    const step = funnel.steps[conversation.stepIndex];
    if (!step) return;
    
    const isFirstMessage = conversation.stepIndex === 0;
    
    const idempotencyKey = 'SEND:' + remoteJid + ':' + conversation.funnelId + ':' + conversation.stepIndex;
    if (checkIdempotency(idempotencyKey)) {
        addLog('STEP_DUPLICATE', 'Passo duplicado ignorado: ' + conversation.funnelId + '[' + conversation.stepIndex + ']');
        return;
    }
    
    addLog('STEP_SEND', `Enviando passo ${conversation.stepIndex} do funil ${conversation.funnelId}`, { 
        step,
        isFirstMessage 
    });
    
    // DELAY ANTES (se configurado)
    if (step.delayBefore && step.delayBefore > 0) {
        addLog('STEP_DELAY', `Aguardando ${step.delayBefore}s antes do passo ${conversation.stepIndex}`);
        await new Promise(resolve => setTimeout(resolve, step.delayBefore * 1000));
    }
    
    // MOSTRAR DIGITANDO (se configurado)
    if (step.showTyping) {
        await sendTypingIndicator(remoteJid);
    }
    
    let result = { success: true };
    
    // PROCESSAR TIPO DO PASSO
    if (step.type === 'delay') {
        const delaySeconds = step.delaySeconds || 10;
        addLog('STEP_DELAY', `Executando delay de ${delaySeconds}s no passo ${conversation.stepIndex}`);
        await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
        
    } else if (step.type === 'typing') {
        const typingSeconds = step.typingSeconds || 3;
        addLog('STEP_TYPING', `Mostrando digitando por ${typingSeconds}s no passo ${conversation.stepIndex}`);
        await sendTypingIndicator(remoteJid, typingSeconds);
        
    } else {
        result = await sendWithFallback(remoteJid, step.type, step.text, step.mediaUrl, isFirstMessage);
    }
    
    if (result.success) {
        conversation.lastSystemMessage = new Date();
        
        if (step.waitForReply && step.type !== 'delay' && step.type !== 'typing') {
            conversation.waiting_for_response = true;
            addLog('STEP_WAITING_REPLY', `Passo ${conversation.stepIndex} aguardando resposta do cliente`, { 
                funnelId: conversation.funnelId, 
                waitForReply: step.waitForReply,
                stepType: step.type
            });
            
            if (step.timeoutMinutes) {
                setTimeout(() => {
                    handleStepTimeout(remoteJid, conversation.stepIndex);
                }, step.timeoutMinutes * 60 * 1000);
            }
            
            conversations.set(remoteJid, conversation);
        } else {
            addLog('STEP_AUTO_ADVANCE', `Passo ${conversation.stepIndex} avançando automaticamente`, { 
                funnelId: conversation.funnelId, 
                waitForReply: step.waitForReply,
                stepType: step.type
            });
            
            conversations.set(remoteJid, conversation);
            await advanceConversation(remoteJid, null, 'auto');
        }
        
        addLog('STEP_SUCCESS', `Passo executado com sucesso: ${conversation.funnelId}[${conversation.stepIndex}]`);
    } else {
        addLog('STEP_FAILED', `Falha no envio do passo: ${result.error}`, { conversation });
    }
}

async function sendTypingIndicator(remoteJid, durationSeconds = 3) {
    const instanceName = stickyInstances.get(remoteJid) || INSTANCES[0];
    
    try {
        await sendToEvolution(instanceName, '/chat/sendPresence', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            presence: 'composing'
        });
        
        addLog('TYPING_START', `Iniciando digitação para ${remoteJid} por ${durationSeconds}s`);
        
        await new Promise(resolve => setTimeout(resolve, durationSeconds * 1000));
        
        await sendToEvolution(instanceName, '/chat/sendPresence', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            presence: 'paused'
        });
        
        addLog('TYPING_END', `Finalizando digitação para ${remoteJid}`);
        
    } catch (error) {
        addLog('TYPING_ERROR', `Erro ao enviar digitação: ${error.message}`, { remoteJid });
    }
}

async function advanceConversation(remoteJid, replyText, reason) {
    const conversation = conversations.get(remoteJid);
    if (!conversation) {
        addLog('ADVANCE_ERROR', `Tentativa de avançar conversa inexistente: ${remoteJid}`);
        return;
    }
    
    // ✅ NOVO: Verificar se conversa foi cancelada
    if (conversation.canceled) {
        addLog('ADVANCE_CANCELED', `Tentativa de avanço em conversa cancelada: ${conversation.funnelId}`, { remoteJid, reason });
        return;
    }
    
    const funnel = funis.get(conversation.funnelId);
    if (!funnel) {
        addLog('ADVANCE_ERROR', `Funil não encontrado: ${conversation.funnelId}`, { remoteJid });
        return;
    }
    
    const currentStep = funnel.steps[conversation.stepIndex];
    if (!currentStep) {
        addLog('ADVANCE_ERROR', `Passo atual não encontrado: ${conversation.stepIndex}`, { 
            remoteJid, 
            funnelId: conversation.funnelId 
        });
        return;
    }
    
    // ✅ NOVO: Capturar contato se marcado no passo
    if (reason === 'reply' && currentStep.captureContact && replyText) {
        const instanceName = stickyInstances.get(remoteJid);
        if (instanceName) {
            captureContact(remoteJid, instanceName, conversation.customerName);
        }
    }
    
    addLog('ADVANCE_START', 'Iniciando avanço da conversa', {
        remoteJid: remoteJid,
        currentStep: conversation.stepIndex,
        funnelId: conversation.funnelId,
        reason: reason,
        currentStepType: currentStep.type,
        waitingForResponse: conversation.waiting_for_response,
        nextOnReply: currentStep.nextOnReply,
        nextOnTimeout: currentStep.nextOnTimeout
    });
    
    let nextStepIndex;
    if (reason === 'reply' && currentStep.nextOnReply !== undefined) {
        nextStepIndex = currentStep.nextOnReply;
        addLog('ADVANCE_LOGIC', `Usando nextOnReply: ${nextStepIndex}`, { reason, currentStep: conversation.stepIndex });
    } else if (reason === 'timeout' && currentStep.nextOnTimeout !== undefined) {
        nextStepIndex = currentStep.nextOnTimeout;
        addLog('ADVANCE_LOGIC', `Usando nextOnTimeout: ${nextStepIndex}`, { reason, currentStep: conversation.stepIndex });
    } else {
        nextStepIndex = conversation.stepIndex + 1;
        addLog('ADVANCE_LOGIC', `Usando próximo sequencial: ${nextStepIndex}`, { reason, currentStep: conversation.stepIndex });
    }
    
    if (nextStepIndex >= funnel.steps.length) {
        addLog('FUNNEL_END', `Funil ${conversation.funnelId} concluído para ${remoteJid}`, {
            totalSteps: funnel.steps.length,
            finalStep: conversation.stepIndex
        });
        
        conversation.waiting_for_response = false;
        conversation.completed = true;
        conversation.completedAt = new Date();
        conversations.set(remoteJid, conversation);
        return;
    }
    
    conversation.stepIndex = nextStepIndex;
    conversation.waiting_for_response = false;
    if (reason === 'reply') {
        conversation.lastReply = new Date();
    }
    
    conversations.set(remoteJid, conversation);
    
    addLog('STEP_ADVANCE', `Avançando para passo ${nextStepIndex} (motivo: ${reason})`, { 
        remoteJid,
        funnelId: conversation.funnelId,
        previousStep: conversation.stepIndex - 1,
        nextStep: nextStepIndex,
        reason: reason
    });
    
    await sendStep(remoteJid);
}

async function handleStepTimeout(remoteJid, expectedStepIndex) {
    const conversation = conversations.get(remoteJid);
    if (!conversation || conversation.stepIndex !== expectedStepIndex || !conversation.waiting_for_response || conversation.canceled) {
        return;
    }
    addLog('STEP_TIMEOUT', `Timeout do passo ${expectedStepIndex} para ${remoteJid}`);
    await advanceConversation(remoteJid, null, 'timeout');
}

// ============ WEBHOOKS ============
app.post('/webhook/kirvano', async (req, res) => {
    try {
        const data = req.body;
        const event = String(data.event || '').toUpperCase();
        const status = String(data.status || data.payment_status || '').toUpperCase();
        const method = String(data.payment?.method || data.payment_method || '').toUpperCase();
        
        const saleId = data.sale_id || data.checkout_id;
        const orderCode = saleId || 'ORDER_' + Date.now();
        const customerName = data.customer?.name || 'Cliente';
        const customerPhone = data.customer?.phone_number || '';
        const totalPrice = data.total_price || 'R$ 0,00';
        
        const remoteJid = phoneToRemoteJid(customerPhone);
        if (!remoteJid || remoteJid === '@s.whatsapp.net') {
            return res.json({ success: false, message: 'Telefone inválido' });
        }
        
        const idempotencyKey = 'KIRVANO:' + event + ':' + remoteJid + ':' + orderCode;
        if (checkIdempotency(idempotencyKey)) {
            return res.json({ success: true, message: 'Evento duplicado ignorado' });
        }
        
        let productType = 'UNKNOWN';
        if (data.products && data.products.length > 0) {
            const offerId = data.products[0].offer_id;
            productType = PRODUCT_MAPPING[offerId] || 'UNKNOWN';
        }
        
        // ✅ NOVO: Incrementar contador diário
        dailyStats.totalEvents++;
        
        addLog('KIRVANO_EVENT', `${event} - ${productType} - ${customerName}`, { orderCode, remoteJid });
        
        let funnelId;
        const isApproved = event.includes('APPROVED') || event.includes('PAID') || status === 'APPROVED';
        const isPix = method.includes('PIX') || event.includes('PIX');
        
        if (isApproved) {
            // ✅ CORREÇÃO CRÍTICA: Cancelar funil PIX antes de iniciar aprovado
            const existingConversation = findConversationByPhone(customerPhone);
            if (existingConversation && existingConversation.funnelId.includes('PIX')) {
                await cancelPixFunnel(remoteJid, 'PAYMENT_APPROVED');
            }
            
            const pixTimeout = pixTimeouts.get(remoteJid);
            if (pixTimeout) {
                clearTimeout(pixTimeout.timeout);
                pixTimeouts.delete(remoteJid);
                addLog('PIX_TIMEOUT_CANCELED', `Timeout cancelado para ${remoteJid}`, { orderCode });
            }
            
            funnelId = productType === 'FAB' ? 'FAB_APROVADA' : 'CS_APROVADA';
            await startFunnel(remoteJid, funnelId, orderCode, customerName, productType, totalPrice);
            
        } else if (isPix) {
            funnelId = productType === 'FAB' ? 'FAB_PIX' : 'CS_PIX';
            
            const existingTimeout = pixTimeouts.get(remoteJid);
            if (existingTimeout) {
                clearTimeout(existingTimeout.timeout);
            }
            
            await startFunnel(remoteJid, funnelId, orderCode, customerName, productType, totalPrice);
            
            const timeout = setTimeout(async () => {
                const conversation = conversations.get(remoteJid);
                if (conversation && conversation.orderCode === orderCode && !conversation.canceled) {
                    const funnel = funis.get(conversation.funnelId);
                    if (funnel && funnel.steps[2]) {
                        conversation.stepIndex = 2;
                        conversation.waiting_for_response = false;
                        conversations.set(remoteJid, conversation);
                        await sendStep(remoteJid);
                    }
                }
                pixTimeouts.delete(remoteJid);
            }, PIX_TIMEOUT);
            
            pixTimeouts.set(remoteJid, { timeout, orderCode, createdAt: new Date() });
        }
        
        res.json({ success: true, message: 'Processado', funnelId });
        
    } catch (error) {
        addLog('KIRVANO_ERROR', error.message, { body: req.body });
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ WEBHOOK EVOLUTION CORRIGIDO COM BUSCA FLEXÍVEL
app.post('/webhook/evolution', async (req, res) => {
    console.log('===== WEBHOOK EVOLUTION RECEBIDO =====');
    console.log(JSON.stringify(req.body, null, 2));
    addLog('WEBHOOK_RECEIVED', 'Webhook Evolution recebido', req.body);
    
    try {
        const data = req.body;
        const messageData = data.data;
        
        if (!messageData || !messageData.key) {
            addLog('WEBHOOK_IGNORED', 'Webhook sem dados de mensagem');
            return res.json({ success: true });
        }
        
        const remoteJid = messageData.key.remoteJid;
        const fromMe = messageData.key.fromMe;
        const messageText = extractMessageText(messageData.message);
        
        addLog('WEBHOOK_DETAILS', 'Processando mensagem', { 
            remoteJid, 
            fromMe, 
            messageText: messageText.substring(0, 100),
            hasConversation: conversations.has(remoteJid)
        });
        
        if (fromMe) {
            addLog('WEBHOOK_FROM_ME', 'Mensagem enviada por nós ignorada', { remoteJid });
            return res.json({ success: true });
        } else {
            const incomingPhone = messageData.key.remoteJid.replace('@s.whatsapp.net', '');
            
            // ✅ CORREÇÃO CRÍTICA: Usar busca flexível
            const conversation = findConversationByPhone(incomingPhone);
            
            if (conversation && conversation.waiting_for_response && !conversation.canceled) {
                const normalizedRemoteJid = normalizePhone(incomingPhone) + '@s.whatsapp.net';
                
                const idempotencyKey = 'REPLY:' + normalizedRemoteJid + ':' + conversation.funnelId + ':' + conversation.stepIndex;
                if (checkIdempotency(idempotencyKey)) {
                    addLog('WEBHOOK_DUPLICATE_REPLY', 'Resposta duplicada ignorada', { remoteJid: normalizedRemoteJid });
                    return res.json({ success: true, message: 'Resposta duplicada' });
                }
                
                addLog('CLIENT_REPLY', 'Resposta recebida e processada', { 
                    originalRemoteJid: remoteJid,
                    normalizedRemoteJid: normalizedRemoteJid,
                    text: messageText.substring(0, 100),
                    step: conversation.stepIndex,
                    funnelId: conversation.funnelId
                });
                
                await advanceConversation(normalizedRemoteJid, messageText, 'reply');
            } else {
                addLog('WEBHOOK_NO_CONVERSATION', 'Mensagem recebida mas sem conversa ativa ou aguardando resposta', { 
                    remoteJid, 
                    incomingPhone,
                    normalizedPhone: normalizePhone(incomingPhone),
                    messageText: messageText.substring(0, 50),
                    conversationFound: !!conversation,
                    conversationWaiting: conversation ? conversation.waiting_for_response : false,
                    conversationCanceled: conversation ? conversation.canceled : false,
                    existingConversations: Array.from(conversations.keys()).slice(0, 3)
                });
            }
        }
        
        res.json({ success: true });
        
    } catch (error) {
        addLog('EVOLUTION_ERROR', error.message, { body: req.body });
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============ API ENDPOINTS ============

// ✅ DASHBOARD MELHORADO COM ESTATÍSTICAS DE MENSAGENS
app.get('/api/dashboard', (req, res) => {
    // Contar uso por instância
    const instanceUsage = {};
    INSTANCES.forEach(inst => {
        instanceUsage[inst] = 0;
    });
    
    stickyInstances.forEach((instance) => {
        if (instanceUsage[instance] !== undefined) {
            instanceUsage[instance]++;
        }
    });
    
    // ✅ CORREÇÃO: Próxima instância baseada na última bem-sucedida
    const nextInstanceIndex = (lastSuccessfulInstanceIndex + 1) % INSTANCES.length;
    const nextInstance = INSTANCES[nextInstanceIndex];
    
    // Contar mensagens enviadas hoje
    const today = new Date().toISOString().split('T')[0];
    if (dailyStats.date !== today) {
        // Reset para novo dia
        dailyStats.date = today;
        dailyStats.firstMessages = new Set();
        dailyStats.totalEvents = 0;
    }
    
    const stats = {
        active_conversations: conversations.size,
        pending_pix: pixTimeouts.size,
        total_funnels: funis.size,
        total_instances: INSTANCES.length,
        sticky_instances: stickyInstances.size,
        last_successful_instance: lastSuccessfulInstanceIndex >= 0 ? INSTANCES[lastSuccessfulInstanceIndex] : 'Nenhuma',
        next_instance_in_queue: nextInstance,
        instance_distribution: instanceUsage,
        conversations_per_instance: Math.round(conversations.size / INSTANCES.length),
        // NOVAS ESTATÍSTICAS
        daily_first_messages: dailyStats.firstMessages.size,
        daily_total_events: dailyStats.totalEvents,
        today_date: today,
        captured_contacts: Array.from(capturedContacts.values()).reduce((total, contacts) => total + contacts.length, 0)
    };
    
    res.json({
        success: true,
        data: stats,
        timestamp: new Date().toISOString()
    });
});

app.get('/api/funnels', (req, res) => {
    const funnelsList = Array.from(funis.values()).map(funnel => ({
        ...funnel,
        isDefault: funnel.id.includes('_APROVADA') || funnel.id.includes('_PIX'),
        stepCount: funnel.steps.length
    }));
    
    res.json({
        success: true,
        data: funnelsList
    });
});

app.post('/api/funnels', (req, res) => {
    const funnel = req.body;
    
    if (!funnel.id || !funnel.name || !funnel.steps) {
        return res.status(400).json({ 
            success: false, 
            error: 'ID, nome e passos são obrigatórios' 
        });
    }
    
    funis.set(funnel.id, funnel);
    addLog('FUNNEL_SAVED', 'Funil salvo: ' + funnel.id);
    
    saveFunnelsToFile();
    
    res.json({ 
        success: true, 
        message: 'Funil salvo com sucesso',
        data: funnel
    });
});

app.delete('/api/funnels/:id', (req, res) => {
    const { id } = req.params;
    
    if (id.includes('_APROVADA') || id.includes('_PIX')) {
        return res.status(400).json({ 
            success: false, 
            error: 'Não é possível excluir funis padrão' 
        });
    }
    
    if (funis.has(id)) {
        funis.delete(id);
        addLog('FUNNEL_DELETED', 'Funil excluído: ' + id);
        
        saveFunnelsToFile();
        
        res.json({ success: true, message: 'Funil excluído com sucesso' });
    } else {
        res.status(404).json({ success: false, error: 'Funil não encontrado' });
    }
});

// ✅ NOVO ENDPOINT: Export de funis
app.get('/api/funnels/export', (req, res) => {
    try {
        const funnelsArray = Array.from(funis.values());
        const exportData = {
            version: '2.0',
            exportDate: new Date().toISOString(),
            totalFunnels: funnelsArray.length,
            funnels: funnelsArray
        };
        
        const filename = `kirvano-funis-backup-${new Date().toISOString().split('T')[0]}.json`;
        
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send(JSON.stringify(exportData, null, 2));
        
        addLog('FUNNELS_EXPORT', `Export realizado: ${funnelsArray.length} funis`, { filename });
        
    } catch (error) {
        addLog('FUNNELS_EXPORT_ERROR', 'Erro no export: ' + error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ NOVO ENDPOINT: Import de funis
app.post('/api/funnels/import', (req, res) => {
    try {
        const importData = req.body;
        
        // Validar estrutura do backup
        if (!importData.funnels || !Array.isArray(importData.funnels)) {
            return res.status(400).json({ 
                success: false, 
                error: 'Arquivo de backup inválido' 
            });
        }
        
        const { funnels } = importData;
        let importedCount = 0;
        let skippedCount = 0;
        
        // Importar cada funil
        funnels.forEach(funnel => {
            if (funnel.id && funnel.name && funnel.steps) {
                funis.set(funnel.id, funnel);
                importedCount++;
                addLog('FUNNEL_IMPORTED', `Funil importado: ${funnel.id}`);
            } else {
                skippedCount++;
                addLog('FUNNEL_IMPORT_SKIP', `Funil inválido ignorado: ${funnel.id || 'sem ID'}`);
            }
        });
        
        // Salvar no arquivo
        saveFunnelsToFile();
        
        addLog('FUNNELS_IMPORT_COMPLETE', `Import concluído: ${importedCount} importados, ${skippedCount} ignorados`);
        
        res.json({ 
            success: true, 
            message: `Import concluído com sucesso!`,
            imported: importedCount,
            skipped: skippedCount,
            total: funnels.length
        });
        
    } catch (error) {
        addLog('FUNNELS_IMPORT_ERROR', 'Erro no import: ' + error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/conversations', (req, res) => {
    const conversationsList = Array.from(conversations.entries()).map(([remoteJid, conv]) => ({
        id: remoteJid,
        phone: remoteJid.replace('@s.whatsapp.net', ''),
        customerName: conv.customerName,
        productType: conv.productType,
        funnelId: conv.funnelId,
        stepIndex: conv.stepIndex,
        waiting_for_response: conv.waiting_for_response,
        createdAt: conv.createdAt,
        lastSystemMessage: conv.lastSystemMessage,
        lastReply: conv.lastReply,
        orderCode: conv.orderCode,
        amount: conv.amount,
        stickyInstance: stickyInstances.get(remoteJid),
        canceled: conv.canceled || false, // ✅ NOVO
        completed: conv.completed || false // ✅ NOVO
    }));
    
    conversationsList.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    
    res.json({
        success: true,
        data: conversationsList
    });
});

// ✅ NOVO ENDPOINT: Contatos capturados
app.get('/api/contacts', (req, res) => {
    const { instance } = req.query;
    
    let contactsData = {};
    
    if (instance && capturedContacts.has(instance)) {
        // Retornar contatos de uma instância específica
        contactsData[instance] = capturedContacts.get(instance);
    } else {
        // Retornar todos os contatos por instância
        capturedContacts.forEach((contacts, instanceName) => {
            contactsData[instanceName] = contacts;
        });
    }
    
    res.json({
        success: true,
        data: contactsData,
        total: Object.values(contactsData).reduce((sum, contacts) => sum + contacts.length, 0)
    });
});

// ✅ NOVO ENDPOINT: Download Excel de contatos
app.get('/api/contacts/download', (req, res) => {
    try {
        const { format = 'google' } = req.query;
        
        if (format === 'google') {
            // ✅ FORMATO ESPECÍFICO PARA GOOGLE CONTACTS
            let csvContent = 'First Name,Mobile Phone\n';
            
            // Coletar todos os contatos de todas as instâncias
            const allContacts = [];
            
            capturedContacts.forEach((contacts, instanceName) => {
                contacts.forEach(contact => {
                    allContacts.push({
                        date: contact.date,
                        phone: contact.phone,
                        capturedAt: contact.capturedAt
                    });
                });
            });
            
            // Ordenar por data de captura (mais recentes primeiro)
            allContacts.sort((a, b) => new Date(b.capturedAt) - new Date(a.capturedAt));
            
            // Remover duplicatas por telefone (manter o mais recente)
            const uniqueContacts = [];
            const seenPhones = new Set();
            
            allContacts.forEach(contact => {
                if (!seenPhones.has(contact.phone)) {
                    seenPhones.add(contact.phone);
                    uniqueContacts.push(contact);
                }
            });
            
            // Gerar CSV no formato Google Contacts
            uniqueContacts.forEach(contact => {
                csvContent += `${contact.date},+${contact.phone}\n`;
            });
            
            const filename = `contatos-google-${new Date().toISOString().split('T')[0]}.csv`;
            
            res.setHeader('Content-Type', 'text/csv; charset=utf-8');
            res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
            res.send('\ufeff' + csvContent); // BOM para UTF-8
            
            addLog('CONTACTS_DOWNLOAD_GOOGLE', `Download Google Contacts: ${uniqueContacts.length} contatos únicos`);
            
        } else if (format === 'detailed') {
            // ✅ FORMATO DETALHADO PARA ANÁLISE
            let csvContent = 'Data,Numero,Nome,Instancia,Capturado_Em\n';
            
            capturedContacts.forEach((contacts, instanceName) => {
                contacts.forEach(contact => {
                    csvContent += `${contact.date},${contact.phone},${contact.name},${instanceName},${contact.capturedAt}\n`;
                });
            });
            
            const filename = `contatos-detalhado-${new Date().toISOString().split('T')[0]}.csv`;
            
            res.setHeader('Content-Type', 'text/csv; charset=utf-8');
            res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
            res.send('\ufeff' + csvContent);
            
            addLog('CONTACTS_DOWNLOAD_DETAILED', 'Download detalhado realizado');
            
        } else {
            res.status(400).json({ success: false, error: 'Formato inválido' });
        }
        
    } catch (error) {
        addLog('CONTACTS_DOWNLOAD_ERROR', 'Erro no download: ' + error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ NOVO ENDPOINT: Limpar contatos
app.delete('/api/contacts', (req, res) => {
    const { instance } = req.query;
    
    if (instance && capturedContacts.has(instance)) {
        capturedContacts.delete(instance);
        addLog('CONTACTS_CLEAR', `Contatos da instância ${instance} removidos`);
    } else {
        capturedContacts.clear();
        addLog('CONTACTS_CLEAR', 'Todos os contatos removidos');
    }
    
    saveContactsToFile();
    
    res.json({ success: true, message: 'Contatos removidos' });
});

app.get('/api/logs', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    const recentLogs = logs.slice(0, limit).map(log => ({
        id: log.id,
        timestamp: log.timestamp,
        type: log.type,
        message: log.message
    }));
    
    res.json({
        success: true,
        data: recentLogs
    });
});

app.post('/api/send-test', async (req, res) => {
    const { remoteJid, type, text, mediaUrl } = req.body;
    
    if (!remoteJid || !type) {
        return res.status(400).json({ 
            success: false, 
            error: 'remoteJid e type são obrigatórios' 
        });
    }
    
    addLog('TEST_SEND', 'Teste de envio: ' + type + ' para ' + remoteJid);
    
    const result = await sendWithFallback(remoteJid, type, text, mediaUrl);
    
    if (result.success) {
        res.json({ 
            success: true, 
            message: 'Mensagem enviada com sucesso!',
            instanceUsed: result.instanceName
        });
    } else {
        res.status(500).json({ success: false, error: result.error });
    }
});

app.get('/api/debug/evolution', async (req, res) => {
    const debugInfo = {
        evolution_base_url: EVOLUTION_BASE_URL,
        evolution_api_key_configured: EVOLUTION_API_KEY !== 'SUA_API_KEY_AQUI',
        evolution_api_key_length: EVOLUTION_API_KEY.length,
        instances: INSTANCES,
        active_conversations: conversations.size,
        sticky_instances_count: stickyInstances.size,
        last_successful_instance: lastSuccessfulInstanceIndex >= 0 ? INSTANCES[lastSuccessfulInstanceIndex] : 'Nenhuma',
        daily_stats: {
            date: dailyStats.date,
            first_messages: dailyStats.firstMessages.size,
            total_events: dailyStats.totalEvents
        },
        captured_contacts: Array.from(capturedContacts.entries()).map(([instance, contacts]) => ({
            instance,
            count: contacts.length
        })),
        test_results: []
    };
    
    try {
        const testInstance = INSTANCES[0];
        const url = EVOLUTION_BASE_URL + '/message/sendText/' + testInstance;
        
        const response = await axios.post(url, {
            number: '5511999999999',
            text: 'teste'
        }, {
            headers: {
                'Content-Type': 'application/json',
                'apikey': EVOLUTION_API_KEY
            },
            timeout: 10000,
            validateStatus: () => true
        });
        
        debugInfo.test_results.push({
            instance: testInstance,
            url: url,
            status: response.status,
            response: response.data,
            headers: response.headers
        });
        
    } catch (error) {
        debugInfo.test_results.push({
            instance: INSTANCES[0],
            error: error.message,
            code: error.code
        });
    }
    
    res.json(debugInfo);
});

// ============ SERVIR FRONTEND ============
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Inicialização - carregar dados persistidos
async function initializeData() {
    console.log('🔄 Carregando dados persistidos...');
    
    const funnelsLoaded = await loadFunnelsFromFile();
    if (!funnelsLoaded) {
        console.log('📋 Usando funis padrão');
    }
    
    const conversationsLoaded = await loadConversationsFromFile();
    if (!conversationsLoaded) {
        console.log('💬 Nenhuma conversa anterior encontrada');
    }
    
    const contactsLoaded = await loadContactsFromFile();
    if (!contactsLoaded) {
        console.log('📞 Nenhum contato anterior encontrado');
    }
    
    const statsLoaded = await loadDailyStatsFromFile();
    if (!statsLoaded) {
        console.log('📊 Estatísticas diárias iniciadas');
    }
    
    console.log('✅ Inicialização concluída');
    console.log('📊 Funis carregados:', funis.size);
    console.log('💬 Conversas ativas:', conversations.size);
    console.log('📞 Contatos capturados:', Array.from(capturedContacts.values()).reduce((total, contacts) => total + contacts.length, 0));
    console.log('📈 Mensagens hoje:', dailyStats.firstMessages.size);
}

// ============ INICIALIZAÇÃO ============
app.listen(PORT, async () => {
    console.log('='.repeat(70));
    console.log('🚀 KIRVANO SYSTEM V2.0 - BACKEND API MELHORADO');
    console.log('='.repeat(70));
    console.log('Porta:', PORT);
    console.log('Evolution:', EVOLUTION_BASE_URL);
    console.log('API Key configurada:', EVOLUTION_API_KEY !== 'SUA_API_KEY_AQUI');
    console.log('Instâncias:', INSTANCES.length);
    console.log('');
    console.log('🔧 CORREÇÕES E MELHORIAS IMPLEMENTADAS:');
    console.log('  ✅ 1. Normalização robusta de telefones');
    console.log('  ✅ 2. Busca flexível de conversas (resolve problema PIX)');
    console.log('  ✅ 3. Sticky instance aprimorado (100% da mesma instância)');
    console.log('  ✅ 4. Cancelamento automático funil PIX ao pagar');
    console.log('  ✅ 5. Contador de mensagens diárias no dashboard');
    console.log('  ✅ 6. Sistema de captura de contatos por instância');
    console.log('  ✅ 7. Download Excel de contatos organizados');
    console.log('  ✅ 8. Logs detalhados para debug');
    console.log('  ✅ 9. Prevenção de funis duplos');
    console.log('  ✅ 10. Persistência completa de dados');
    console.log('');
    console.log('🎯 PROBLEMAS RESOLVIDOS:');
    console.log('  🔹 PIX que parava no meio (normalização números)');
    console.log('  🔹 Mensagens de instâncias diferentes (sticky 100%)');
    console.log('  🔹 Funil PIX + Aprovado simultâneos (cancelamento)');
    console.log('  🔹 Falta controle mensagens enviadas (contador diário)');
    console.log('  🔹 Captura contatos para Google Contacts');
    console.log('');
    console.log('📡 API Endpoints:');
    console.log('  GET  /api/dashboard        - Stats + Contadores diários');
    console.log('  GET  /api/funnels          - Listar funis');
    console.log('  POST /api/funnels          - Criar/editar funil');
    console.log('  GET  /api/conversations    - Listar conversas');
    console.log('  GET  /api/contacts         - Contatos capturados');
    console.log('  GET  /api/contacts/download - Download CSV contatos');
    console.log('  DELETE /api/contacts       - Limpar contatos');
    console.log('  GET  /api/logs             - Logs recentes');
    console.log('  POST /api/send-test        - Teste de envio');
    console.log('  GET  /api/debug/evolution  - Debug Evolution API');
    console.log('');
    console.log('📨 Webhooks:');
    console.log('  POST /webhook/kirvano      - Eventos Kirvano (melhorado)');
    console.log('  POST /webhook/evolution    - Eventos Evolution (corrigido)');
    console.log('');
    console.log('🌐 Frontend: http://localhost:' + PORT);
    console.log('🧪 Testes: http://localhost:' + PORT + '/test.html');
    console.log('='.repeat(70));
    
    // Carregar dados persistidos
    await initializeData();
});
