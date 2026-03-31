-- Extensions
create extension if not exists "pgcrypto";

-- Customers table
create table if not exists public.customers (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    email text null,
    created_at timestamptz not null default now(),
    constraint customers_email_unique unique (email)
);

-- Licenses table
create table if not exists public.licenses (
    id uuid primary key default gen_random_uuid(),
    customer_id uuid not null references public.customers (id) on delete cascade,
    license_key_hash text not null unique,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null,
    last_seen_at timestamptz null,
    notes text null,
    constraint licenses_expires_after_created check (expires_at >= created_at + interval '30 days')
);

-- License activations table
create table if not exists public.license_activations (
    id uuid primary key default gen_random_uuid(),
    license_id uuid not null references public.licenses (id) on delete cascade,
    activated_at timestamptz not null default now(),
    client_fingerprint text null,
    ip text null,
    user_agent text null
);

-- App configuration key/value store
create table if not exists public.app_config (
    key text primary key,
    value jsonb not null,
    updated_at timestamptz not null default now()
);

-- Indexes
create index if not exists idx_licenses_customer_id on public.licenses (customer_id);
create index if not exists idx_licenses_expires_at on public.licenses (expires_at);
create index if not exists idx_license_activations_license_id on public.license_activations (license_id);

-- Row Level Security: enabled and deny-by-default
alter table public.customers enable row level security;
alter table public.licenses enable row level security;
alter table public.license_activations enable row level security;

alter table public.customers force row level security;
alter table public.licenses force row level security;
alter table public.license_activations force row level security;
