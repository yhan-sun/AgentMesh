import React, { useState, useRef, useEffect } from "react";
import { ChevronDown, Check } from "lucide-react";

export interface SelectOption {
  value: string;
  label: string;
  subLabel?: string;
  badge?: string;
  badgeType?: "ready" | "running" | "completed" | "pending" | "failed";
  icon?: React.ReactNode;
}

interface CustomSelectProps {
  options: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
}

export const CustomSelect: React.FC<CustomSelectProps> = ({
  options,
  value,
  onChange,
  placeholder = "Select an option...",
  className = "",
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const selectedOption = options.find((opt) => opt.value === value);

  useEffect(() => {
    const handleOutsideClick = (e: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener("mousedown", handleOutsideClick);
    }
    return () => {
      document.removeEventListener("mousedown", handleOutsideClick);
    };
  }, [isOpen]);

  const handleSelect = (val: string) => {
    onChange(val);
    setIsOpen(false);
  };

  return (
    <div
      ref={containerRef}
      className={`custom-select-container ${className}`}
      style={{ position: "relative", width: "100%" }}
    >
      {/* Trigger Button */}
      <div
        className={`custom-select-trigger ${isOpen ? "open" : ""}`}
        onClick={() => setIsOpen(!isOpen)}
        tabIndex={0}
        role="button"
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            setIsOpen(!isOpen);
          }
        }}
      >
        <div className="trigger-content">
          {selectedOption ? (
            <div className="selected-item">
              {selectedOption.icon && (
                <span className="item-icon">{selectedOption.icon}</span>
              )}
              <div className="item-labels">
                <span className="item-main-label">{selectedOption.label}</span>
                {selectedOption.subLabel && (
                  <span className="item-sub-label">
                    {selectedOption.subLabel}
                  </span>
                )}
              </div>
              {selectedOption.badge && (
                <span
                  className={`badge badge-${
                    selectedOption.badgeType || "ready"
                  } item-badge`}
                >
                  {selectedOption.badge}
                </span>
              )}
            </div>
          ) : (
            <span className="placeholder">{placeholder}</span>
          )}
        </div>
        <ChevronDown
          size={16}
          className={`chevron-icon ${isOpen ? "rotated" : ""}`}
        />
      </div>

      {/* Dropdown Menu */}
      {isOpen && (
        <div className="custom-select-dropdown">
          {options.length === 0 ? (
            <div className="dropdown-empty">No options available</div>
          ) : (
            options.map((option) => {
              const isSelected = option.value === value;
              return (
                <div
                  key={option.value}
                  className={`dropdown-option ${isSelected ? "selected" : ""}`}
                  onClick={() => handleSelect(option.value)}
                >
                  <div className="option-left">
                    {option.icon && (
                      <span className="item-icon">{option.icon}</span>
                    )}
                    <div className="option-text">
                      <span className="option-label">{option.label}</span>
                      {option.subLabel && (
                        <span className="option-sub-label">
                          {option.subLabel}
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="option-right">
                    {option.badge && (
                      <span
                        className={`badge badge-${
                          option.badgeType || "ready"
                        } item-badge`}
                      >
                        {option.badge}
                      </span>
                    )}
                    {isSelected && <Check size={14} className="check-icon" />}
                  </div>
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
};
