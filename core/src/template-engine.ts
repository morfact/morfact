// template-engine.ts — renders {{expression}} against a render context.
// Supports: {{params.film_id}}, {{tasks.*.output.*}}, {{i+1}}, {{route}}, {{groupIndex}}

export interface RenderContext {
  params?: Record<string, unknown>;
  tasks?: Record<string, unknown>;
  flow?: { variables?: Record<string, unknown> };
  vars?: Record<string, unknown>;
  i?: number;
  item?: unknown;
  route?: unknown;
  groupIndex?: number;
  [key: string]: unknown;
}

type TemplateToken = { kind: 'text'; value: string } | { kind: 'expr'; raw: string };

function tokenize(template: string): TemplateToken[] {
  const tokens: TemplateToken[] = [];
  let remaining = template;
  while (remaining.length > 0) {
    const m = remaining.match(/\{\{(.+?)\}\}/);
    if (!m || m.index === undefined) {
      tokens.push({ kind: 'text', value: remaining });
      break;
    }
    if (m.index > 0) {
      tokens.push({ kind: 'text', value: remaining.slice(0, m.index) });
    }
    const raw = m[1];
    if (raw !== undefined) {
      tokens.push({ kind: 'expr', raw: raw.trim() });
    }
    remaining = remaining.slice(m.index + m[0].length);
  }
  return tokens;
}

const SAFE_BUILTINS: Record<string, unknown> = Object.freeze({
  Math: Object.freeze(Math),
  String,
  Number,
  Boolean,
  Array,
  parseInt,
  parseFloat,
  isNaN,
  isFinite,
  Infinity,
  NaN,
  undefined,
});

function buildSafeEval(code: string, ctx: Record<string, unknown>): () => unknown {
  const keys = Object.keys(ctx);
  const values = keys.map((k) => ctx[k]);
  const fn = new Function(...keys, ...Object.keys(SAFE_BUILTINS), `return (${code});`);
  return () => fn(...values, ...Object.values(SAFE_BUILTINS));
}

function resolveValue(expr: string, ctx: RenderContext): unknown {
  const trim = expr.trim();

  // Array wildcard: tasks['page-script'][*].output.text
  if (trim.includes('[*]')) {
    const wildMatch = trim.match(/^(.+?)\[\*\](.*)$/);
    if (wildMatch) {
      const basePath = wildMatch[1];
      const suffixPath = wildMatch[2];
      if (basePath === undefined) return [];
      const baseArray = resolveValue(basePath, ctx);
      if (!Array.isArray(baseArray)) return [];
      if (!suffixPath || suffixPath === '') return baseArray;
      const suffix = suffixPath.startsWith('.') ? suffixPath.slice(1) : suffixPath;
      return baseArray.map((item, idx) =>
        resolveValue(`item.${suffix}`, { ...ctx, item, i: idx }),
      );
    }
  }

  const safeContext: Record<string, unknown> = {
    params: ctx.params ?? {},
    tasks: ctx.tasks ?? {},
    flow: ctx.flow ?? { variables: {} },
    vars: ctx.vars ?? ctx.flow?.variables ?? {},
    i: ctx.i,
    item: ctx.item,
    route: ctx.route,
    groupIndex: ctx.groupIndex,
  };

  try {
    const fn = buildSafeEval(trim, safeContext);
    return fn();
  } catch {
    return undefined;
  }
}

export function renderTemplate(
  template: unknown,
  context: RenderContext,
): unknown {
  if (typeof template === 'string') {
    return renderString(template, context);
  }
  if (Array.isArray(template)) {
    return template.map((item) => renderTemplate(item, context));
  }
  if (template && typeof template === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(
      template as Record<string, unknown>,
    )) {
      result[key] = renderTemplate(value, context);
    }
    return result;
  }
  return template;
}

function renderString(str: string, context: RenderContext): unknown {
  const tokens = tokenize(str);
  if (tokens.length === 0) return str;

  const first = tokens[0];
  if (first === undefined) return str;

  // If it's a single {{expr}}, return the raw value (preserve type)
  if (tokens.length === 1 && first.kind === 'expr') {
    return resolveValue(first.raw, context);
  }

  const parts = tokens.map((tok) => {
    if (tok.kind === 'text') return tok.value;
    const val = resolveValue(tok.raw, context);
    if (val === undefined || val === null) return '';
    if (typeof val === 'object' || Array.isArray(val)) {
      try {
        return JSON.stringify(val);
      } catch {
        return String(val);
      }
    }
    return String(val);
  });
  return parts.join('');
}

export function evaluateWhen(
  condition: string,
  context: RenderContext,
): boolean {
  try {
    const result = resolveValue(condition, context);
    return Boolean(result);
  } catch {
    return false;
  }
}

export function parseForeachExpression(
  foreachExpr: string,
  context: RenderContext,
): { items: unknown[]; isRange: boolean } {
  const trimmed = foreachExpr.trim();

  // Form 1: var in range(start, end)
  const rangeMatch = trimmed.match(/^(\w+)\s+in\s+range\((.+?),\s*(.+?)\)$/);
  if (rangeMatch) {
    const startExpr = rangeMatch[2];
    const endExpr = rangeMatch[3];
    if (startExpr === undefined || endExpr === undefined) {
      throw new Error(`Invalid range expression: ${foreachExpr}`);
    }
    const start = resolveValue(startExpr.trim(), context);
    const end = resolveValue(endExpr.trim(), context);
    if (typeof start !== 'number' || typeof end !== 'number') {
      throw new Error(`Invalid range: start=${start}, end=${end}`);
    }
    const items = Array.from({ length: end - start }, (_, i) => start + i);
    return { items, isRange: true };
  }

  // Form 2: var in array_expression
  const arrayMatch = trimmed.match(/^(\w+)\s+in\s+(.+)$/);
  if (arrayMatch) {
    const arrayExpr = arrayMatch[2];
    if (arrayExpr === undefined) {
      throw new Error(`Invalid array expression: ${foreachExpr}`);
    }
    const items = resolveValue(arrayExpr.trim(), context);
    if (!Array.isArray(items)) {
      throw new Error(`Foreach expression does not evaluate to array: ${arrayExpr}`);
    }
    return { items, isRange: false };
  }

  throw new Error(`Invalid foreach expression: ${foreachExpr}`);
}
