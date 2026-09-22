// Copyright (c) 2026 Harllan He. Licensed under MIT.
import { useRef, useState, type ReactNode } from 'react'
import { toast } from 'sonner'
import { useTranslation } from 'react-i18next'
import { Gauge, Monitor, Moon, Pencil, Server, ShieldCheck, Sun, type LucideIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { PageHead } from '@/components/page-head'
import {
  useLoadBalancingMode, useSetLoadBalancingMode,
  useAuthKeys, useSetAuthKeys,
  useCacheSplitRatio, useSetCacheSplitRatio,
  useConcurrencyConfig, useSetConcurrencyConfig,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { LANG_STORAGE_KEY } from '@/i18n'
import type { Theme } from '@/hooks/use-theme'

/** 分区标题（设计稿 .set-cap） */
const SET_CAP =
  'flex items-center gap-1.5 px-0.5 pb-2 text-[10.5px] font-semibold uppercase tracking-[.08em] text-ink-3'
/** 分区卡片（设计稿 .set-card） */
const SET_CARD = 'overflow-hidden rounded-lg border border-hairline bg-surface shadow-hair'
/** 配置行（设计稿 .set-row） */
const SET_ROW = 'flex items-center gap-4 border-b border-hairline px-4 py-[13px] last:border-b-0'
/** 行键名（设计稿 .set-k） */
const SET_K = 'flex items-center gap-[7px] text-[12.5px] font-semibold'
/** 行说明（设计稿 .set-d） */
const SET_D = 'mt-0.5 max-w-[560px] text-[11px] leading-[1.5] text-ink-3'
/** 行控件区（设计稿 .set-c） */
const SET_C = 'ml-auto flex flex-none items-center gap-2'
/** 只读值框（设计稿 .field.w-s） */
const FIELD_S =
  'flex h-[31px] min-w-[96px] items-center rounded-[7px] border border-hairline-2 bg-surface-2 px-2.5 font-mono text-[12px] text-ink-2'
/** 分段器容器（设计稿 .seg，8px 圆角与 borderRadius.lg 的 11px 不同，故用字面量） */
const SEG = 'flex flex-none gap-0.5 rounded-[8px] bg-surface-3 p-0.5'
/** 分段器档位（设计稿 .seg button） */
const SEG_BTN =
  'flex h-[27px] items-center gap-[5px] whitespace-nowrap rounded-[6px] px-2.5 text-[12px] font-medium text-ink-2 transition-colors hover:text-ink focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand disabled:pointer-events-none disabled:opacity-50'
/** 分段器激活档（设计稿 .seg button.on） */
const SEG_BTN_ON = 'bg-surface font-semibold text-ink shadow-hair'
/** 开关（设计稿 .sw） */
const SW =
  'relative h-[18px] w-8 flex-none rounded-[10px] border border-transparent transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand'
/** 开关滑块（设计稿 .sw i）：白色不随主题反转，与设计稿一致 */
const SW_DOT = 'absolute top-0.5 size-3 rounded-full bg-white shadow-[0_1px_2px_rgba(0,0,0,.28)]'
/** 管理密码掩码：固定 12 位，不随真实长度变化以免泄露长度信息 */
const PSW_MASK = '∗'.repeat(12)

/** 配置分区：`.set-cap` 标题 + `.set-card` 卡片 */
function Section({ icon: Icon, title, children }: { icon: LucideIcon; title: string; children: ReactNode }) {
  return (
    <section>
      <div className={SET_CAP}>
        <Icon className="size-3.5" aria-hidden="true" />
        {title}
      </div>
      <div className={SET_CARD}>{children}</div>
    </section>
  )
}

/** 配置行：左列键名 + 说明，右列控件 */
function Row({ label, desc, children }: { label: string; desc: string; children: ReactNode }) {
  return (
    <div className={SET_ROW}>
      <div className="min-w-0">
        <div className={SET_K}>{label}</div>
        <div className={SET_D}>{desc}</div>
      </div>
      <div className={SET_C}>{children}</div>
    </div>
  )
}

interface SegOption<T extends string> {
  value: T
  label: string
  icon?: LucideIcon
}

/**
 * 两档互斥选择（设计稿 .seg）。点击已激活档不回调，
 * 使 onSelect 可安全绑定 toggle 式（无参）的状态切换函数 —— 该等价性仅在两档下成立，
 * 故 options 类型收紧为二元组：新增第三档会在此处直接编译报错，
 * 迫使调用方改用显式设值而非 toggle。
 */
function Seg<T extends string>({
  groupLabel,
  value,
  options,
  onSelect,
  disabled,
}: {
  groupLabel: string
  value: T | undefined
  options: readonly [SegOption<T>, SegOption<T>]
  onSelect: (next: T, x?: number, y?: number) => void
  disabled?: boolean
}) {
  const rootRef = useRef<HTMLDivElement>(null)
  const selectedIdx = options.findIndex((o) => o.value === value)

  /** APG radiogroup：方向键从当前焦点档位移并同步选中，焦点跟随目标档 */
  const move = (from: number, delta: number) => {
    const to = (from + delta + options.length) % options.length
    rootRef.current?.querySelectorAll('button')[to]?.focus()
    const next = options[to]
    if (next && next.value !== value) onSelect(next.value)
  }

  return (
    <div ref={rootRef} className={SEG} role="radiogroup" aria-label={groupLabel}>
      {options.map((opt, i) => {
        const on = opt.value === value
        const Icon = opt.icon
        return (
          <button
            key={opt.value}
            type="button"
            role="radio"
            aria-checked={on}
            disabled={disabled}
            // roving tabindex：整组只占一个 Tab 位；value 未加载时回退到首档以免整组无法聚焦
            tabIndex={on || (selectedIdx < 0 && i === 0) ? 0 : -1}
            onKeyDown={(e) => {
              if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                e.preventDefault()
                move(i, -1)
              } else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                e.preventDefault()
                move(i, 1)
              }
            }}
            onClick={(e) => {
              if (!on) onSelect(opt.value, e.clientX, e.clientY)
            }}
            className={`${SEG_BTN} ${on ? SEG_BTN_ON : ''}`}
          >
            {Icon && <Icon className="size-3.5" aria-hidden="true" />}
            {opt.label}
          </button>
        )
      })}
    </div>
  )
}

/** 布尔开关（设计稿 .sw） */
function Sw({ label, on, onToggle }: { label: string; on: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={on}
      aria-label={label}
      onClick={onToggle}
      className={`${SW} ${on ? 'bg-brand' : 'bg-track'}`}
    >
      <span className={`${SW_DOT} ${on ? 'right-0.5' : 'left-0.5'}`} />
    </button>
  )
}

interface SettingsPanelProps {
  /** 主题与折叠态由 Dashboard 持有：useTheme() 每实例独立 state，
   *  本页自行调用会与侧栏页脚按钮各持一份而无法同步 */
  theme: Theme
  onToggleTheme: (x?: number, y?: number) => void
  sidebarCollapsed: boolean
  onToggleSidebarCollapsed: () => void
}

export function SettingsPanel({
  theme,
  onToggleTheme,
  sidebarCollapsed,
  onToggleSidebarCollapsed,
}: SettingsPanelProps) {
  const { t, i18n } = useTranslation()
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()
  const { data: authKeysData, isLoading: isLoadingAuthKeys } = useAuthKeys()
  const { mutate: setAuthKeysMut, isPending: isSettingAuthKeys } = useSetAuthKeys()
  const [adminPswDraft, setAdminPswDraft] = useState('')
  const [editingAdminPsw, setEditingAdminPsw] = useState(false)
  const { data: cacheSplitData, isLoading: isLoadingCacheSplit } = useCacheSplitRatio()
  const { mutate: setCacheSplitMut, isPending: isSettingCacheSplit } = useSetCacheSplitRatio()
  const [cacheSplitDraft, setCacheSplitDraft] = useState('')
  const [editingCacheSplit, setEditingCacheSplit] = useState(false)
  const { data: concurrencyData, isLoading: isLoadingConcurrency } = useConcurrencyConfig()
  const { mutate: setConcurrencyMut, isPending: isSettingConcurrency } = useSetConcurrencyConfig()
  const [concurrencyDraft, setConcurrencyDraft] = useState<Record<string, string> | null>(null)

  const lang = i18n.language === 'en' ? 'en' : 'zh'

  const changeLanguage = (next: 'zh' | 'en') => {
    i18n.changeLanguage(next)
    localStorage.setItem(LANG_STORAGE_KEY, next)
  }

  const changeMode = (next: 'priority' | 'balanced') => {
    setLoadBalancingMode(next, {
      onSuccess: () =>
        toast.success(
          t('settings.switchedTo', {
            mode: next === 'priority' ? t('settings.priorityMode') : t('settings.balancedMode'),
          })
        ),
      onError: (e) => toast.error(extractErrorMessage(e)),
    })
  }

  const saveAdminPsw = () => {
    setAuthKeysMut(
      { adminPsw: adminPswDraft.trim() },
      {
        onSuccess: () => {
          toast.success(t('settings.adminPasswordUpdated'))
          setEditingAdminPsw(false)
          setAdminPswDraft('')
        },
        onError: (e) => toast.error(extractErrorMessage(e)),
      }
    )
  }

  const saveCacheSplit = () => {
    const next = Number(cacheSplitDraft.trim())
    // 后端同样会拒绝越界值，这里先拦一道，省去一次往返
    if (!Number.isFinite(next) || next < 0 || next >= 1) {
      toast.error(t('settings.cacheSplitRatioInvalid'))
      return
    }
    setCacheSplitMut(next, {
      onSuccess: (data) => {
        toast.success(
          data.ratio > 0
            ? t('settings.cacheSplitRatioEffective', { value: data.effectiveMultiplier.toFixed(4) })
            : t('settings.cacheSplitRatioOff')
        )
        setEditingCacheSplit(false)
        setCacheSplitDraft('')
      },
      onError: (e) => toast.error(extractErrorMessage(e)),
    })
  }

  return (
    <div>
      <PageHead
        crumb={[t('dashboard.navSystem'), t('settings.title')]}
        title={t('settings.title')}
        note={t('settings.headNote')}
      />

      <div className="flex flex-col gap-5 pb-[26px]">
        <Section icon={Server} title={t('settings.capService')}>
          <Row label={t('settings.loadBalancingMode')} desc={t('settings.selectStrategyDesc')}>
            <Seg
              groupLabel={t('settings.loadBalancingMode')}
              value={loadBalancingData?.mode}
              options={[
                { value: 'priority' as const, label: t('settings.priorityMode') },
                { value: 'balanced' as const, label: t('settings.balancedMode') },
              ]}
              onSelect={changeMode}
              disabled={isLoadingMode || isSettingMode}
            />
          </Row>

          <Row label={t('settings.cacheSplitRatio')} desc={t('settings.cacheSplitRatioDesc')}>
            {editingCacheSplit ? (
              <>
                <Input
                  type="text"
                  inputMode="decimal"
                  autoFocus
                  placeholder={t('settings.cacheSplitRatioPlaceholder')}
                  value={cacheSplitDraft}
                  onChange={(e) => setCacheSplitDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') saveCacheSplit()
                  }}
                  className="w-[200px]"
                />
                <Button size="sm" disabled={!cacheSplitDraft.trim() || isSettingCacheSplit} onClick={saveCacheSplit}>
                  {t('common.save')}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setEditingCacheSplit(false)
                    setCacheSplitDraft('')
                  }}
                >
                  {t('common.cancel')}
                </Button>
              </>
            ) : (
              <>
                <div className={FIELD_S}>
                  {isLoadingCacheSplit
                    ? t('common.loading')
                    : !cacheSplitData || cacheSplitData.ratio <= 0
                      ? t('settings.cacheSplitRatioOff')
                      : `${(cacheSplitData.ratio * 100).toFixed(2)}% · ${t('settings.cacheSplitRatioEffective', { value: cacheSplitData.effectiveMultiplier.toFixed(4) })}`}
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={isLoadingCacheSplit}
                  onClick={() => {
                    setCacheSplitDraft(cacheSplitData ? String(cacheSplitData.ratio) : '')
                    setEditingCacheSplit(true)
                  }}
                >
                  <Pencil />
                  {t('common.edit')}
                </Button>
              </>
            )}
          </Row>
        </Section>

        <Section icon={Gauge} title={t('settings.capConcurrency')}>
          <Row label={t('settings.maxInflight')} desc={t('settings.maxInflightDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.maxInflightPerCredential ?? (concurrencyData ? String(concurrencyData.maxInflightPerCredential) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), maxInflightPerCredential: e.target.value })} />
          </Row>
          <Row label={t('settings.backoffBase')} desc={t('settings.backoffBaseDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.backoffBaseMs ?? (concurrencyData ? String(concurrencyData.backoffBaseMs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), backoffBaseMs: e.target.value })} />
          </Row>
          <Row label={t('settings.backoffMax')} desc={t('settings.backoffMaxDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.backoffMaxMs ?? (concurrencyData ? String(concurrencyData.backoffMaxMs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), backoffMaxMs: e.target.value })} />
          </Row>
          <Row label={t('settings.backoffMult')} desc={t('settings.backoffMultDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.backoffMultiplier ?? (concurrencyData ? String(concurrencyData.backoffMultiplier) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), backoffMultiplier: e.target.value })} />
          </Row>
          <Row label={t('settings.suspendedBackoff')} desc={t('settings.suspendedBackoffDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.suspendedBackoffMs ?? (concurrencyData ? String(concurrencyData.suspendedBackoffMs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), suspendedBackoffMs: e.target.value })} />
          </Row>
          <Row label={t('settings.globalFirst')} desc={t('settings.globalFirstDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.globalFirstPauseSecs ?? (concurrencyData ? String(concurrencyData.globalFirstPauseSecs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), globalFirstPauseSecs: e.target.value })} />
          </Row>
          <Row label={t('settings.globalSecond')} desc={t('settings.globalSecondDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.globalSecondPauseSecs ?? (concurrencyData ? String(concurrencyData.globalSecondPauseSecs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), globalSecondPauseSecs: e.target.value })} />
          </Row>
          <Row label={t('settings.globalThirdMin')} desc={t('settings.globalThirdDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.globalThirdPauseMinSecs ?? (concurrencyData ? String(concurrencyData.globalThirdPauseMinSecs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), globalThirdPauseMinSecs: e.target.value })} />
          </Row>
          <Row label={t('settings.globalThirdMax')} desc={t('settings.globalThirdDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.globalThirdPauseMaxSecs ?? (concurrencyData ? String(concurrencyData.globalThirdPauseMaxSecs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), globalThirdPauseMaxSecs: e.target.value })} />
          </Row>
          <Row label={t('settings.globalReset')} desc={t('settings.globalResetDesc')}>
            <Input className="w-[120px]" disabled={isLoadingConcurrency || isSettingConcurrency}
              value={concurrencyDraft?.globalResetIdleSecs ?? (concurrencyData ? String(concurrencyData.globalResetIdleSecs) : '')}
              onChange={(e) => setConcurrencyDraft({ ...(concurrencyDraft || {}), globalResetIdleSecs: e.target.value })} />
          </Row>
          <Row label={t('settings.saveConcurrency')} desc={t('settings.concurrencyHelpDesc')}>
            <Button size="sm" disabled={!concurrencyData || isSettingConcurrency} onClick={() => {
              if (!concurrencyData) return
              const d = concurrencyDraft || {}
              const num = (k: keyof typeof concurrencyData, fb: number) => {
                const n = Number(d[k] ?? concurrencyData[k])
                return Number.isFinite(n) ? n : fb
              }
              setConcurrencyMut({
                maxInflightPerCredential: num('maxInflightPerCredential', 5),
                backoffBaseMs: num('backoffBaseMs', 500),
                backoffMaxMs: num('backoffMaxMs', 3000),
                backoffMultiplier: num('backoffMultiplier', 1.5),
                suspendedBackoffMs: num('suspendedBackoffMs', 1000),
                globalCooldownEnabled: concurrencyData.globalCooldownEnabled,
                globalFirstPauseSecs: num('globalFirstPauseSecs', 5),
                globalSecondPauseSecs: num('globalSecondPauseSecs', 15),
                globalThirdPauseMinSecs: num('globalThirdPauseMinSecs', 30),
                globalThirdPauseMaxSecs: num('globalThirdPauseMaxSecs', 60),
                globalResetIdleSecs: num('globalResetIdleSecs', 120),
              }, {
                onSuccess: () => { toast.success(t('settings.concurrencySaved')); setConcurrencyDraft(null) },
                onError: (e) => toast.error(extractErrorMessage(e)),
              })
            }}>{t('common.save')}</Button>
          </Row>
        </Section>

        <Section icon={ShieldCheck} title={t('settings.capSecurity')}>
          <Row label={t('settings.adminPassword')} desc={t('settings.adminPasswordHint')}>
            {editingAdminPsw ? (
              <>
                <Input
                  type="text"
                  autoFocus
                  placeholder={t('settings.adminPasswordPlaceholder')}
                  value={adminPswDraft}
                  onChange={(e) => setAdminPswDraft(e.target.value)}
                  className="w-[290px]"
                />
                <Button size="sm" disabled={!adminPswDraft.trim() || isSettingAuthKeys} onClick={saveAdminPsw}>
                  {t('common.save')}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setEditingAdminPsw(false)
                    setAdminPswDraft('')
                  }}
                >
                  {t('common.cancel')}
                </Button>
              </>
            ) : (
              <>
                <div className={FIELD_S}>
                  {isLoadingAuthKeys ? t('common.loading') : authKeysData?.adminPsw ? PSW_MASK : '—'}
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={isLoadingAuthKeys}
                  onClick={() => {
                    setAdminPswDraft('')
                    setEditingAdminPsw(true)
                  }}
                >
                  <Pencil />
                  {t('common.edit')}
                </Button>
              </>
            )}
          </Row>
        </Section>

        <Section icon={Monitor} title={t('settings.capUi')}>
          <Row label={t('settings.language')} desc={t('settings.languageDesc')}>
            <Seg
              groupLabel={t('settings.language')}
              value={lang}
              options={[
                { value: 'zh' as const, label: t('settings.languageZh') },
                { value: 'en' as const, label: t('settings.languageEn') },
              ]}
              onSelect={changeLanguage}
            />
          </Row>
          <Row label={t('settings.theme')} desc={t('settings.themeDesc')}>
            <Seg
              groupLabel={t('settings.theme')}
              value={theme}
              options={[
                { value: 'light' as const, label: t('settings.themeLight'), icon: Sun },
                { value: 'dark' as const, label: t('settings.themeDark'), icon: Moon },
              ]}
              onSelect={() => onToggleTheme()}
            />
          </Row>
          <Row label={t('settings.sidebarCollapsed')} desc={t('settings.sidebarCollapsedDesc')}>
            <Sw
              label={t('settings.sidebarCollapsed')}
              on={sidebarCollapsed}
              onToggle={onToggleSidebarCollapsed}
            />
          </Row>
        </Section>
      </div>
    </div>
  )
}
