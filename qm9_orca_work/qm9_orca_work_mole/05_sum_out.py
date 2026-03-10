import os
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime

# ======================
# 配置学术风格（参考提供的折线图风格：无网格、黑色边框、简洁）
# ======================
# 使用默认风格 + 手动关闭网格
# plt.style.use('seaborn-v0_8-white')   # 如果可用，可用这个白底风格
# 或者直接用 default 并调整

plt.rcParams['font.family'] = 'DejaVu Sans'          # 或 'Arial', 'Helvetica' 如果服务器有
plt.rcParams['font.size'] = 11
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 13
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10
plt.rcParams['legend.fontsize'] = 10
plt.rcParams['figure.dpi'] = 150
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['axes.linewidth'] = 1.0          # 轴边框加粗到1.0，更接近黑色边框感
plt.rcParams['xtick.major.width'] = 1.0
plt.rcParams['ytick.major.width'] = 1.0
plt.rcParams['xtick.direction'] = 'in'
plt.rcParams['ytick.direction'] = 'in'
plt.rcParams['xtick.top'] = True              # 上边有刻度（许多学术图这样）
plt.rcParams['ytick.right'] = True            # 右边有刻度
plt.rcParams['axes.grid'] = False             # 明确关闭网格

# 基础路径
base_dir = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output"

# ======================
# 统计完成计算数量
# ======================
counts = defaultdict(int)

for dirname in os.listdir(base_dir):
    if not dirname.startswith('orca_out_'):
        continue
    if 'mkl' in dirname.lower():
        continue

    method_basis = dirname.replace('orca_out_', '').strip()
    full_dir = os.path.join(base_dir, dirname)

    if not os.path.isdir(full_dir):
        continue

    completed_files = [
        f for f in os.listdir(full_dir)
        if f.endswith('.out') and f.startswith('dsgdb9nsd_')
    ]

    counts[method_basis] = len(completed_files)
    print(counts[method_basis])
# ======================
# 排序（降序）
# ======================
sorted_data = sorted(counts.items(), key=lambda x: x[1], reverse=True)
if not sorted_data:
    print("没有找到任何符合条件的输出文件。")
    exit()

labels = [item[0] for item in sorted_data]
values = [item[1] for item in sorted_data]

# ======================
# 绘图
# ======================
fig, ax = plt.subplots(figsize=(9, 5.5))

# 颜色序列（学术常用鲜明但不刺眼颜色，可按需调整）
colors = ['#E69F00', '#56B4E9', '#009E73', '#F0E442',
          '#0072B2', '#D55E00', '#CC79A7', '#999999']

bars = ax.bar(labels, values,
              color=colors[:len(labels)],
              width=0.72,
              edgecolor='black',
              linewidth=1.1)          # 黑色边框更明显

# 标题和标签（简洁、正式）
ax.set_title("Number of Successfully Completed ORCA Calculations",
             pad=16, fontsize=13, weight='medium')
ax.set_xlabel("Method + Basis Set", labelpad=12)
ax.set_ylabel("Number of .out Files", labelpad=12)

# x轴标签旋转
plt.xticks(rotation=40, ha='right', rotation_mode='anchor')

# 在柱顶显示数值
max_val = max(values) if values else 1
for bar in bars:
    height = bar.get_height()
    if height > 0:
        ax.text(bar.get_x() + bar.get_width()/2., height + max_val*0.012,
                f'{int(height)}',
                ha='center', va='bottom',
                fontsize=10)

# 去掉顶部和右侧的spine（可选，更干净，但参考图保留了所有边框）
# ax.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)

# 紧凑布局
plt.tight_layout()

# 保存（高分辨率，适合论文）
# plt.savefig("orca_completed_calculations.pdf", bbox_inches='tight', dpi=600)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
plt.savefig(f"./pngs/orca_completed_calculations_{timestamp}.png", bbox_inches='tight', dpi=1200)

plt.show()
